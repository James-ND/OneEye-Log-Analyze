import pandas as pd
import numpy as np
import argparse
import os
import sys

def parse_total_url_by_duration(url_info):
    urls = url_info.keys()
    new_dfs = []

    for url in urls:
        df_info = url_info[url].sort_values("Duration")[-1:]
        new_dfs.append(df_info)

    new_df = pd.concat(new_dfs).sort_values("Duration")
    return new_df

def parse_single_url(url, url_df, total_count):
    url_stat = {}
    url_stat['count'] = len(url_df)
    url_stat['usage'] = round (len(url_df) / total_count, 4)
    url_stat['duration_min'] = url_df.Duration.min()
    url_stat['duration_max'] = url_df.Duration.max()
    url_stat['duration_mean'] = url_df.Duration.mean()
    url_stat['duration_std'] = url_df.Duration.std()

    return url_stat

def main(csv_file, count, stat):
    df = pd.read_csv(csv_file, 
                     sep=',', 
                     names=[
                         'Method', 
                         'Url', 
                         'Stauts',
                         'Duration', 
                         'DB_usage', 
                         'Thrift_usage', 
                         'TS'
                    ],
                    dtype={
                        'Method': str,
                        'Url': str,
                        'Status': str,
                        'Duration': np.float64,
                        'DB_usage': np.float64,
                        'Thrift_usage': np.float64,
                        'TS': str
                    }
                )
    total_count = len(df)
    df_grp = df.groupby('Url')
    url_info = {}
    url_usage = {}
    for n, g in df_grp:
        url_info[n] = g
        url_usage[n] = len(g)
    
    new_df = parse_total_url_by_duration(url_info)
    if count > len(new_df):
        count = len(new_df)
    stat_info = {}

    if stat.lower() == 'duration':
        top_df = new_df[-count:]
        top_index = top_df.index.values.tolist()
        for i in range(len(top_index)-1, -1, -1):
            url = top_df['Url'].get(top_index[i])
            url_stat = parse_single_url(url, url_info[url], total_count)
            stat_info[url] = url_stat
    elif stat == 'usage':
        sorted_url_usage = sorted(url_usage.items(), key=lambda obj:obj[1], reverse=True)
        for i in range(count):
            url = sorted_url_usage[i][0]
            url_stat = parse_single_url(url, url_info[url], total_count)
            stat_info[url] = url_stat

    return stat_info

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Analyze OneEye logs')
    parser.add_argument('-f', '--file', help = 'One-eye log file in csv format.')
    parser.add_argument('-c', '--count', default=50, help = 'Count of top URL to analyze.')
    parser.add_argument('-s', '--stat', default='Duration', help = 'Method of statistic (Duration|Usage')
    
    args = parser.parse_args()

    if not args.file:
        print("Please specify the log file to analyze!")
        sys.exit(1)

    main(args.file, int(args.count), args.stat)