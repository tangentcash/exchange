import { Log } from './logging';
import { Exchange } from './service/exchange';
import { Jobs } from './service/jobs';
import { Blockchain } from './service/blockchain';
import { Relay } from './service/relay';
import { Common } from './common';
import { BigNumber } from 'bignumber.js';
import { Quotes } from './service/market';
import fs from 'fs';

BigNumber.config({ DECIMAL_PLACES: 18, ROUNDING_MODE: 1 });

async function main(): Promise<void> {
    let config;
    try {
        config = JSON.parse(fs.readFileSync(process.argv[2]).toString('utf8'));
        if (typeof config != 'object')
            throw new Error('config must be an object');
    } catch (exception) {
        Log.error('path', process.argv[2] || null, ' failed to load a config:', exception);
        return process.exit(1);
    }

    const options = Common.args(config);
    try {
        const log: any = options.log || { };
        Log.info(`logging stream setup (info: ${(log.info ? 'specified' : 'auto')}, error: ${(log.error ? 'specified' : 'auto')}, query: ${(log.query ? 'specified' : 'auto')})`);
        Log.setup({
            infoPath: log.info || './log/info.log',
            errorPath: log.error || './log/error.log',
            queryPath: log.query || './log/query.log'
        });
    } catch (exception: any) {
        Log.error('logging setup error:', exception);
        return process.exit(1);
    }

    try {
        const exchange: any = options.exchange || { };
        Log.info('exchange client setup (' + (exchange.database || 'db') + ': ' + (exchange.host || '?') + ':' + (exchange.port || '?') + ')');
        await Exchange.setup(exchange);
    } catch (exception: any) {
        Log.error('exchange setup error:', exception);
        return process.exit(1);
    }

    try {
        const blockchain: any = options.blockchain || { };
        Log.info('blockchain client setup (network: ' + (blockchain.network || 'default') + ', validator: ' + (blockchain.validator || 'null') + ')');
        Log.info('blockchain event sources:', blockchain.markets || { });
        await Blockchain.setup({
            validator: blockchain.validator,
            markets: blockchain.markets,
            network: blockchain.network || undefined
        });
    } catch (exception: any) {
        Log.error('blockchain setup error:', exception);
        return process.exit(1);
    }

    try {
        Log.info('running background jobs');
        if (options.jobs?.blockSync === true) {
            await Blockchain.sync();
        }
        if (typeof options.jobs?.assetsCleanup == 'number') {
            await Jobs.runAssetCleanup(options.jobs.assetsCleanup);
        }
        if (typeof options.jobs?.pairsCleanup == 'number') {
            await Jobs.runPairCleanup(options.jobs.pairsCleanup);
        }
        if (typeof options.jobs?.marketData == 'number') {
            await Jobs.runMarketData(options.jobs.marketData);
        }
        if (typeof options.jobs?.assetPrices == 'object') {
            const setup = options.jobs.assetPrices;
            if (typeof setup.realtime == 'object' && typeof setup.fallback == 'object' && typeof setup.frequency == 'number') {
                Quotes.setSources({
                    realtime: setup.realtime || { },
                    fallback: setup.fallback || { }
                });
                Jobs.runAssetPrices(setup.frequency);
                Log.info('background jobs: asset price resolver now running');
            }
        }
        Log.info('background jobs: OK');
    } catch (exception: any) {
        Log.error('background jobs error:', exception);
        return process.exit(1);
    }

    try {
        const relay: any = options.relay || { };
        Log.info('relay server setup (http: ' + (relay.host || '?') + ':' + (relay.port || '?') + ')');
        await Relay.setup({
            host: relay.host,
            port: relay.port || 19420
        });
    } catch (exception: any) {
        Log.error('relay setup error:', exception);
        return process.exit(1);
    }

    const shutdown = async () => {
        try {
            Log.info('relay server shutdown');
            await Relay.shutdown();
        } catch (exception: any) {
            Log.error('relay shutdown error:', exception);
        }
        
        try {
            Log.info('blockchain client shutdown');
            await Blockchain.shutdown();
        } catch (exception: any) {
            Log.error('blockchain shutdown error:', exception);
        }
                
        try {
            Log.info('exchange client shutdown');
            await Exchange.shutdown();
        } catch (exception: any) {
            Log.error('exchange shutdown error:', exception);
        }

        process.exit(0);
    }
    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
}

main();