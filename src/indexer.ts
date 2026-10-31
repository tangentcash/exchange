import { Log } from './logging';
import { Exchange } from './service/exchange';
import { Jobs } from './service/jobs';
import { Blockchain } from './service/blockchain';
import { Relay } from './service/relay';
import { Peers } from './service/peers';
import { Common } from './common';
import { ContractType } from './types';
import { BigNumber } from 'bignumber.js';
import { Quotes } from './service/market';
import fs from 'fs';
import { Chain } from 'tangentsdk';

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

    const blockchain: any = options.blockchain || { };
    if (typeof blockchain.network == 'string' && ['regtest', 'testnet', 'mainnet'].includes(blockchain.network))
        Chain.props = (Chain as unknown as Record<string, typeof Chain.props>)[blockchain.network];

    try {
        const exchange: any = options.exchange || { };
        Log.info('exchange client setup (' + (exchange.database || 'db') + ': ' + (exchange.host || '?') + ':' + (exchange.port || '?') + ')');
        await Exchange.setup(exchange);
    } catch (exception: any) {
        Log.error('exchange setup error:', exception);
        return process.exit(1);
    }

    let rescanContracts: string[] = [ ];
    try {
        Log.info('running blockchain boot check');
        const configured: Record<string, string> = blockchain.contracts || { };
        const contracts = await Exchange.getContracts();
        for (let type in configured) {
            const account = configured[type];
            if (typeof account != 'string' || account.length == 0 || !Object.values(ContractType).includes(type as ContractType))
                continue;
            const row = contracts.find((v) => v.type == type && v.account == account);
            if (row == null ? contracts.length > 0 : !row.synced)
                rescanContracts.push(account);
        }
        if (contracts.length == 0)
            Log.info('blockchain boot check: no contracts registered yet (the first sync will index from scratch)');
        else if (rescanContracts.length == 0)
            Log.info('blockchain boot check: configured contracts match the database (no rescan needed)');
        else
            Log.info('blockchain boot check: targeted contract rescan queued for ' + rescanContracts.length + ' contract(s): ' + rescanContracts.join(', '));
    } catch (exception: any) {
        Log.error('blockchain boot check error:', exception);
        return process.exit(1);
    }
    try {
        Log.info('blockchain client setup (network: ' + (blockchain.network || 'default') + ', validator: ' + (blockchain.validator || 'null') + ')');
        await Blockchain.setup({
            validator: blockchain.validator,
            contracts: blockchain.contracts,
            network: blockchain.network || undefined,
            rescan: rescanContracts
        });
    } catch (exception: any) {
        Log.error('blockchain setup error:', exception);
        return process.exit(1);
    }

    try {
        Log.info('running background jobs');
        if (options.jobs?.blockSync === true) {
            if (await Blockchain.sync()) {
                await Exchange.setContractsSynced();
                Log.info('background jobs: initial block sync complete (contracts marked synced, event-driven from now on)');
            } else {
                Log.info('background jobs: initial block sync incomplete (contracts left unsynced, event-driven from now on)');
            }
        }
        if (typeof options.jobs?.assetsCleanup == 'number') {
            await Jobs.runAssetCleanup(options.jobs.assetsCleanup);
            Log.info('background jobs: asset cleanup now running');
        }
        if (typeof options.jobs?.pairsCleanup == 'number') {
            await Jobs.runPairCleanup(options.jobs.pairsCleanup);
            Log.info('background jobs: pair cleanup now running');
        }
        if (typeof options.jobs?.marketData == 'number') {
            await Jobs.runMarketData(options.jobs.marketData);
            Log.info('background jobs: market polling now running');
        }
        if (typeof options.jobs?.amsDeposits == 'object') {
            await Jobs.runPreattestationsData(options.jobs.amsDeposits);
            Log.info('background jobs: preattestation polling now running');
        }
        if (typeof options.jobs?.assetPrices == 'object') {
            const setup = options.jobs.assetPrices;
            if (typeof setup.realtime == 'object' && typeof setup.fallback == 'object' && typeof setup.frequency == 'number') {
                Quotes.setSources({
                    realtime: setup.realtime || { },
                    fallback: setup.fallback || { },
                    currency: setup.currency || { },
                    logging: setup.logging || false
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
        if (Array.isArray(options.peers)) {
            Log.info('price peers setup (trusted peers: ' + options.peers.length + ')');
            Peers.setupClients(options.peers);
        }
    } catch (exception) {
        Log.error('price peers setup error:', exception);
        return process.exit(1);
    }

    try {
        const relay: any = options.relay || { };
        Log.info('relay server setup (http: ' + (relay.host || '?') + ':' + (relay.port || '?') + ')');
        await Relay.setup({
            host: relay.host,
            port: relay.port || 19420,
            ipSource: relay.ipSource,
            rateLimits: relay.rateLimits
        });
    } catch (exception: any) {
        Log.error('relay setup error:', exception);
        return process.exit(1);
    }

    const shutdown = async () => {
        try {
            Log.info('price peers shutdown');
            await Peers.shutdown();
        } catch (exception) {
            Log.error('price peers shutdown error:', exception);
        }

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