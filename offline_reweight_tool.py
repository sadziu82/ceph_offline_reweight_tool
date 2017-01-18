#!/usr/bin/python

# PYTHON_ARGCOMPLETE_OK

##
import os
import re
import sys
import time
import json
import numpy
import shutil
import pprint
import signal
import argparse
import subprocess
import argcomplete

##
GB = 1024*1024*1024
TB = GB*1024


##
finished = False


##
class NotEnoughPGMemberError(BaseException):
    pass

##
def log(text):
    print text

##
def parse_osd_df(osd_df_path):
    osd_df = {}
    with open(osd_df_path, 'r') as fh:
        df = json.loads(fh.read())
        osd_df = df['nodes']
    return osd_df

##
def parse_ceph_df(ceph_df_path, pools=[]):
    ceph_df = {}
    try:
        with open(ceph_df_path, 'r') as fh:
            df = json.loads(fh.read())
            total_used = 0
            for pool in df['pools']:
                if len(pools) == 0 or pool['name'] in pools:
                    total_used += pool['stats']['bytes_used']
            for pool in df['pools']:
                if len(pools) == 0 or pool['name'] in pools:
                    ceph_df[pool['name']] = {
                        'id': pool['id'],
                        'weight': round(pool['stats']['bytes_used'] / float(total_used), 2),
                    }
    except IOError:
        for p in ceph_df_path.split(','):
            pool = p.split(':')
            if len(pools) == 0 or pool[0] in pools:
                ceph_df[pool[0]] = {
                    'id': int(pool[1]),
                    'weight': round(int(pool[2]) / 100.0, 2),
                }
    return ceph_df

##
def set_initial_crush_weights(crushmap, osd_df, choose_total_tries=None):
    if choose_total_tries is not None:
        cmd = 'crushtool -i {} -o {} --set-choose-total-tries {}'.format(
                crushmap, crushmap, choose_total_tries)
        ##
        log('setting "choose_total_tries" tunable to {}'.format(choose_total_tries))
        subprocess.check_output(cmd, shell=True, preexec_fn=os.setpgrp)

    for osd in osd_df:
        new_osd_weight = round(osd['kb'] / float(GB), 5)
        cmd = 'crushtool -i {} -o {} --reweight-item osd.{} {}'.format(
                crushmap, crushmap, osd['id'], new_osd_weight)
        subprocess.check_output(cmd, shell=True, preexec_fn=os.setpgrp)

##
def prepare_final_crush_map(crushmap, weights):
    final_crushmap = 'offline_crush_{}'.format(time.time())
    shutil.copy(crushmap, final_crushmap)
    for osd in osd_df:
        new_osd_weight = round(weights[str(osd['id'])]['crush_weight'], 5)
        cmd = 'crushtool -i {} -o {} --reweight-item osd.{} {}'.format(
                final_crushmap, final_crushmap, osd['id'], new_osd_weight)
        crushtool = subprocess.check_output(cmd, shell=True, preexec_fn=os.setpgrp)
    return final_crushmap

##
def map_pgs(osdmap, crushmap, pool_id=None):
    ##
    weights = {}
    pg_stats = {
        'min_pg': 100000000,
        'max_pg': 0,
        'mapped_min': 99,
        'mapped_max': 0,
    }
    ##
    cmd = 'osdmaptool {} --import-crush {} --test-map-pgs-dump --mark-up-in --clear-temp 2>/dev/null'.format(
            osdmap, crushmap)
    if pool_id is not None:
        cmd += ' --pool {}'.format(pool_id)
    osdmaptool = subprocess.check_output(cmd, shell=True, preexec_fn=os.setpgrp)
    ##
    for line in osdmaptool.split('\n'):
        m = re.match(r'^osd\.(?P<id>\d+)\s+(?P<pg_count>\d+)\s+(?P<first>\d+)\s+(?P<primary>\d+)\s+(?P<crush_weight>\d+(\.\d+)?)\s+(?P<weight>\d+(\.\d+)?)$', line)
        k = re.match(r'^\s+avg\s+(?P<avg>\d+)\s+stddev\s+(?P<stddev>\d+(\.\d+)?)\s+', line)
        p = re.match(r'^\s*(?P<pg>[0-9a-fA-F]+\.[0-9a-fA-F]+)\s+\[(?P<upset>[\d,]+)\]\s+(?P<primary>\d+)\s*$', line)
        if m:
            weights[m.group('id')] = {
                'pg_count': float(m.group('pg_count')),
                'crush_weight': float(m.group('crush_weight')),
            }
            if float(m.group('pg_count')) > pg_stats['max_pg']:
                pg_stats['max_pg'] = float(m.group('pg_count'))
            if float(m.group('pg_count')) < pg_stats['min_pg']:
                pg_stats['min_pg'] = float(m.group('pg_count'))
        elif k:
            pg_stats['avg']  = float(k.group('avg'))
            pg_stats['stddev'] = float(k.group('stddev'))
        elif p:
            size = len(p.group('upset').split(','))
            if pg_stats['mapped_min'] > size:
                pg_stats['mapped_min'] = size
            if pg_stats['mapped_max'] < size:
                pg_stats['mapped_max'] = size
    ##
    if pg_stats['mapped_min'] != pg_stats['mapped_max']:
        raise NotEnoughPGMemberError('unable to find enough OSD for some '
                'PG, pool_id == {}, mapped(min, max) == ({}, {})'.format(
                    pool_id, pg_stats['mapped_min'], pg_stats['mapped_max']))
    return (weights, pg_stats)

##
def update_crush_weights(crushmap, old_weights, avg_pg_count, change_step):
    stats = {
        'up': 0,
        'down': 0,
    }
    for osd_id in old_weights:
        osd = old_weights[osd_id]
        if osd['pg_count'] < avg_pg_count:
            new_osd_weight = osd['crush_weight'] * (1.0 + change_step)
            cmd = 'crushtool -i {} -o {} --reweight-item osd.{} {}'.format(
                    crushmap, crushmap, osd_id, new_osd_weight)
            crushtool = subprocess.check_output(cmd, shell=True, preexec_fn=os.setpgrp)
            stats['up'] += 1
        elif osd['pg_count'] > avg_pg_count:
            new_osd_weight = osd['crush_weight'] * (1.0 - change_step)
            cmd = 'crushtool -i {} -o {} --reweight-item osd.{} {}'.format(
                    crushmap, crushmap, osd_id, new_osd_weight)
            crushtool = subprocess.check_output(cmd, shell=True, preexec_fn=os.setpgrp)
            stats['down'] += 1
    return stats

##
def find_optimal_osd_crush_weights_for_pool(ceph_df, osd_df, osdmap, crushmap, pool_id,
        target_stddev, max_rounds, pg_min_max_diff, change_step=0.005):
    ##
    global finished
    last_stddev = 999999
    round_no = 0
    ## prepare crushmap copy to operate on
    tmp_crushmap = 'offline_crush_{}.tmp'.format(time.time())
    shutil.copy(crushmap, tmp_crushmap)
    ##
    while not finished and round_no < args.max_rounds:
        round_no += 1
        if round_no == 1:
            (weights, pg_stats) = map_pgs(osdmap, tmp_crushmap, pool_id=pool_id)
        ##
        if pg_stats['stddev'] <= target_stddev or (pg_stats['max_pg'] - pg_stats['min_pg']) <= args.pg_min_max_diff:
            break
        ## change weight and update stats
        update_stats = update_crush_weights(tmp_crushmap, weights, pg_stats['avg'], change_step)
        (weights, pg_stats) = map_pgs(osdmap, tmp_crushmap, pool_id=pool_id)
        ## print progress info
        sys.stdout.write('\rpool: {:3.0f}, round: {:5.0f}, stddev: {:8.4f}, '
                'up: {:4.0f}, down: {:4.0f}, min_pg: {:4.0f}, max_pg: {:4.0f}'.
                format(pool_id, round_no, pg_stats['stddev'],
                    update_stats['up'], update_stats['down'],
                    pg_stats['min_pg'], pg_stats['max_pg']))
        sys.stdout.flush()
    ## clear progress line
    sys.stdout.write('\r{}\r'.format(' ' * 100))
    ## prepare final stats
    (weights, pg_stats) = map_pgs(osdmap, tmp_crushmap, pool_id=pool_id)
    os.unlink(tmp_crushmap)
    return (weights, pg_stats)

##
def find_optimal_osd_crush_weights(ceph_df, osd_df, osdmap, crushmap, pools,
        target_stddev, max_rounds, pg_min_max_diff, change_step=0.005):
    ##
    weights = {}
    pg_stats = {}
    for pool in pools:
        (weights[pool],
         pg_stats[pool]) = find_optimal_osd_crush_weights_for_pool(
                ceph_df=ceph_df, osd_df=osd_df, osdmap=osdmap,
                crushmap=crushmap, pool_id=ceph_df[pool]['id'],
                target_stddev=args.target_stddev, max_rounds=args.max_rounds,
                pg_min_max_diff=args.pg_min_max_diff)
    weights['FINAL'] = {}
    for pool in pools:
        for osd_id in weights[pool]:
            weights['FINAL'].setdefault(osd_id, {
                    'crush_weight': 0, 'pg_count': 0 })
            weights['FINAL'][osd_id]['crush_weight'] += \
                weights[pool][osd_id]['crush_weight'] * ceph_df[pool]['weight']
            weights['FINAL'][osd_id]['pg_count'] += \
                weights[pool][osd_id]['pg_count']
    return (weights, pg_stats)

##
def calculate_target_space_usage(osdmap, crushmap, pg_dump):
    ##
    osd_stats = {}
    pg_stats = {}
    ##
    with open(pg_dump, 'r') as fh:
        pgs = json.loads(fh.read())
        for stats in pgs['pg_stats']:
            pg_stats[stats['pgid']] = stats['stat_sum']['num_bytes']
    ##
    cmd = 'osdmaptool {} --import-crush {} --test-map-pgs-dump --mark-up-in --clear-temp 2>/dev/null'.format(
            osdmap, crushmap)
    osdmaptool = subprocess.check_output(cmd, shell=True, preexec_fn=os.setpgrp)
    ##
    for line in osdmaptool.split('\n'):
        p = re.match(r'^\s*(?P<pg>[0-9a-fA-F]+\.[0-9a-fA-F]+)\s+\[(?P<upset>[\d,]+)\]\s+(?P<primary>\d+)\s*$', line)
        if p:
            for osd_id in p.group('upset').split(','):
                osd_stats.setdefault(osd_id, {'primary_pgs': 0, 'used_bytes': 0})
                osd_stats[osd_id]['used_bytes'] += pg_stats[p.group('pg')]
            osd_stats[p.group('primary')]['primary_pgs'] += 1
    ##
    return osd_stats


##
def display_detailed_stats(osdmap, crushmap, final_crushmap, pg_dump):
    pg_dump_stats_before = calculate_target_space_usage(osdmap=osdmap, crushmap=crushmap,
        pg_dump=args.pg_dump)
    pg_dump_stats_after = calculate_target_space_usage(osdmap=osdmap, crushmap=final_crushmap,
        pg_dump=args.pg_dump)
    stddev = {
        'use_before': [],
        'use_after': [],
        'primary_pgs_before': [],
        'primary_pgs_after': [],
    }
    for osd_id in sorted(pg_dump_stats_after.keys(), cmp=lambda x, y: cmp(int(x), int(y))):
        primary_pgs_before = pg_dump_stats_before[osd_id]['primary_pgs']
        primary_pgs_after = pg_dump_stats_after[osd_id]['primary_pgs']
        stddev['primary_pgs_before'].append(primary_pgs_before)
        stddev['primary_pgs_after'].append(primary_pgs_after)
        ##
        use_before = float(pg_dump_stats_before[osd_id]['used_bytes']) / TB
        use_after = float(pg_dump_stats_after[osd_id]['used_bytes']) / TB
        stddev['use_before'].append(use_before)
        stddev['use_after'].append(use_after)
        ##
        pct_diff = 0
        if use_before > use_after:
            pct_diff = -1.0 + (use_after / use_before)
        elif use_before < use_after:
            pct_diff = 1.0 - (use_before / use_after)
        pct_diff *= 100
        ##
        ##
        print 'OSD: {:>4}, used space[TB]: {:6.2f} -> {:6.2f}, diff: {:5.0f}%, primary PGs: {:6d} -> {:6d}'.format(osd_id,
                use_before, use_after, pct_diff, primary_pgs_before, primary_pgs_after)
    print '\n--- STDDEV, used space[--]: {:6.2f} -> {:6.2f}, ----: {:>5}%, primary PGs: {:6.2f} -> {:6.2f}'.format(
            numpy.std(stddev['use_before']),  numpy.std(stddev['use_after']), '---',
            numpy.std(stddev['primary_pgs_before']),  numpy.std(stddev['primary_pgs_after']))


##
def exit_handler(signum, frame):
    global finished
    finished = True



##
if __name__ == '__main__':
    ##
    signal.signal(signal.SIGINT, exit_handler)
    ##
    parser = argparse.ArgumentParser()
    parser.add_argument('osd_df', help='path to osd df (json)',
            default=None, type=str)
    parser.add_argument('ceph_df', help='path to ceph df (json)',
            default=None, type=str)
    parser.add_argument('osdmap', help='path to osdmap (binary)',
            default=None, type=str)
    parser.add_argument('crushmap', help='path to crushmap (binary)',
            default=None, type=str)
    parser.add_argument('pools', help='pools used to calculate distribution',
            default=None, type=str, nargs='+')
    parser.add_argument('--target-stddev', help='target stddev',
            default=1.0, type=float)
    parser.add_argument('--initial-change-step', help='change step in %%',
            default=500, type=int)
    parser.add_argument('--max-rounds', help='max number of round',
            default=1000, type=int)
    parser.add_argument('--pg-min-max-diff', help='max acceptable difference beetween min_pg and max_pg',
            default=0, type=int)
    parser.add_argument('--choose-total-tries',
            default=None, type=int,
            help='set choose_total_tries tunable, default do not change')
    parser.add_argument('--pg-dump',
            default=None, type=str,
            help='load pg dump to calculate target space based on PGs size')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    ##
    osdmap = args.osdmap
    original_crushmap = args.crushmap
    ## prepare crushmap copy to operate on
    crushmap = 'offline_crush_{}.tmp'.format(time.time())
    shutil.copy(original_crushmap, crushmap)
    ##
    pools = args.pools
    osd_df = parse_osd_df(args.osd_df)
    ceph_df = parse_ceph_df(args.ceph_df)
    set_initial_crush_weights(crushmap, osd_df, choose_total_tries=args.choose_total_tries)
    ## start calculations
    (weights, pg_stats) = find_optimal_osd_crush_weights(ceph_df=ceph_df, osd_df=osd_df,
            osdmap=osdmap, crushmap=crushmap, pools=pools,
            target_stddev=args.target_stddev, max_rounds=args.max_rounds,
            pg_min_max_diff=args.pg_min_max_diff)
    ## prepare final crushmap
    final_crushmap = prepare_final_crush_map(crushmap=crushmap,
            weights=weights['FINAL'])
    ##
    print '\nto apply run:\n\tceph osd setcrushmap -i {}\n'.format(final_crushmap)
    ##
    if args.pg_dump:
        display_detailed_stats(osdmap, original_crushmap, final_crushmap, args.pg_dump)
