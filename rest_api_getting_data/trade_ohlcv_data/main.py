from get_data_victor import get_data_combined_exch_concurrent


if __name__ == '__main__':
    start = '2022-10-01T00:00:00.000Z'
    end = '2022-10-05T00:00:00.000Z'

    list_pairs = ['weth-crv',
                  'weth-usdt',
                  'weth-tribe',
                  'weth-renbtc',
                  'weth-rari',
                  'weth-amp',
                  'weth-raini',
                  'weth-polk',
                  'weth-enj',
                  'weth-atri',
                  'weth-dip',
                  'weth-zrx',
                  'weth-superf',
                  'weth-ads',
                  'weth-uos',
                  'weth-inj',
                  'weth-cqt',
                  'weth-rail',
                  'weth-swap',
                  'weth-vra',
                  'weth-xcur',
                  'weth-pre',
                  'weth-reef',
                  'weth-gtc',
                  'weth-seth2',
                  'weth-saito',
                  'weth-gods',
                  'weth-imx',
                  'weth-ens',
                  'weth-plu']

    list_exchanges = ['curv', 'sush', 'usp2', 'usp3', 'blc2']
    interval = '1h'

    new_list_exchanges = [list_exchanges for pair in list_pairs]

    start_dates = [start for i in range(len(list_pairs))]
    end_dates = [end for i in range(len(list_pairs))]

    b = get_data_combined_exch_concurrent(exches=new_list_exchanges, pairs=list_pairs, start_dates=start,
                                          end_dates=end, intervals=interval, data_type='trades',
                                          max_workers_process=20,
                                          max_workers_thread=20)
    print(b)
