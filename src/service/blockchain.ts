import { AssetId, Chain, Spot, Pubkeyhash, RPC, Signing, Types, Uint256 } from "tangentsdk";
import { Log } from "./../logging";
import { Exchange } from "./exchange";
import { BigNumber } from "bignumber.js";

export type Options = {
    validator?: string;
    contracts?: Record<string, Record<string, string>>;
    network?: 'regtest' | 'testnet' | 'mainnet'
}

export type Events = {
    orderEvent: number;
    tradeEvent: number;
}

export type BlockchainInfo = AssetId & {
    divisibility: BigNumber,
    transactionFinality: BigNumber,
    transactionExpires: boolean,
    compositionPolicy: string,
    tokenPolicy: string,
    routingPolicy: string
}

export type EventInfo = {
    hash: string;
    fromAccount: string;
    toAccount: string;
    method: string | null;
    args: any[],
    pays: { asset: AssetId, value: BigNumber }[];
    event: { type: number, args: any[] };
};

export class Blockchain {
    static contracts: {
        versions: Record<string, Record<string, string>>;
        accounts: Record<string, { version: string, type: string }>;
        topics: string[];
    } = { versions: { }, accounts: { }, topics: [] };
    static blockchains: BlockchainInfo[] | null = null;
    static timer: number | null = null;
    static syncing: boolean;

    static accountOf(account: string): ({ account: string, version: string, type: string }) | null {
        const result = this.contracts.accounts[account];
        return result ? { account: account, ...result } : null;
    }
    static async setup(config: Options): Promise<void> {
        if (config.network && ['regtest', 'testnet', 'mainnet'].includes(config.network))
            Chain.props = Chain[config.network];

        if (typeof config.contracts == 'object') {
            this.contracts.accounts = { };
            this.contracts.versions = config.contracts;
            this.contracts.topics = Object.keys(this.contracts.versions).map(x => this.contracts.versions[x]).map((x) => [x.dex, x.dlp]).flat().filter(x => x != null);
            for (let version in this.contracts.versions) {
                const types = this.contracts.versions[version];
                for (let type in types) {
                    this.contracts.accounts[types[type]] = { version: version, type: type };
                }
            }
        }
        RPC.applyValidator(config.validator || null);
        RPC.applyImplementation({
            onNodeMessage: (method: string, message: { args: any; error: unknown; } | { args: any; result: any; }, _: number) => Log.query(`blockchain rpc`, method, message),
        });
        return await this.keepAlive(true);
    }
    static async shutdown(): Promise<void> {
        if (this.timer != null) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        if (RPC.socket != null)
            await RPC.disconnectSocket();
    }
    static async keepAlive(healthCheck: boolean = false, hadConnection: boolean = false): Promise<void> {
        try {
            if (!RPC.socket || !hadConnection) {
                await RPC.unsubscribeTopics();
                const result = await RPC.subscribeTopics(this.contracts.topics, true);
                if (!RPC.socket || result == null) {
                    throw new Error('rpc unreachable');
                } else if (!hadConnection) {
                    Log.info('blockchain rpc acquired (topics: ' + (result != null ? result.toString() : 'null') + ')');
                    hadConnection = true;
                }
            }
        } catch (exception) {
            Log.error('blockchain rpc connection error:', exception);
            hadConnection = false;
        }
        if (healthCheck) {
            this.timer = setTimeout(() => this.keepAlive(healthCheck, hadConnection), 5000) as any;
        }
    }
    static async sync(tipBlockNumber?: number): Promise<void> {
        if (!this.contracts.topics.length || this.syncing)
            return;

        this.syncing = true;
        try {
            let pendingQueue: number[] = [];
            let syncedBlock = await Exchange.getLatestBlock();
            for (let i = 0; i < this.contracts.topics.length; i++) {
                const queue: number[] = [];
                while (true) {
                    const transactions = await RPC.getTransactionsByOwner(this.contracts.topics[i], queue.length, queue.length > 0 ? 32 : 1, 0, 2);
                    if (!Array.isArray(transactions) || !transactions.length)
                        break;
                    
                    let finalize = false;
                    for (let i = 0; i < transactions.length && !finalize; i++) {
                        const blockNumber = parseInt(transactions[i].receipt.block_number.toString());
                        if (syncedBlock && blockNumber <= syncedBlock.blockNumber) {
                            let collision = 0;
                            const child = blockNumber == syncedBlock.blockNumber ? syncedBlock : await Exchange.getBlockByNumber(blockNumber);
                            const parent = child ? await this.findBlock(child.blockHash) : null;
                            finalize = child && parent;
                            while (!finalize) {
                                const tipBlock = await Exchange.getLatestBlock(collision++);
                                const collisionBlock = tipBlock ? await this.findBlock(tipBlock.blockHash) : null;
                                if (!tipBlock || collisionBlock != null) {
                                    Log.info(`blockchain reorganize: ${tipBlock ? 'rollback to block ' + tipBlock.blockNumber.toString() : 'rebuild from scratch'} (collision: ${tipBlock ? tipBlock.blockHash.toHex() : 'null'})`);
                                    await Exchange.rollbackToBlock(tipBlock?.blockNumber || 0);
                                    syncedBlock = tipBlock;
                                    break;
                                }
                            }
                        }
                        if (!finalize)
                            queue.unshift(blockNumber);
                    }
                    
                    if (queue.length > 0)
                        Log.info(`blockchain indexing: ${queue.length} transaction${queue.length > 1 ? 's' : ''}`);
                    
                    if (finalize)
                        break;
                }
                pendingQueue = [...pendingQueue, ...queue];
            }

            pendingQueue.sort((a: any, b: any) => a - b);
            for (let i = 0; i < pendingQueue.length; i++) {
                const blockNumber = pendingQueue[i];
                await this.dispatchBlock(blockNumber);
                Log.info(`blockchain sync progress: ${(100 * (i + 1) / pendingQueue.length).toFixed(2)}% (block: ${blockNumber})`);
            }
            
            if (tipBlockNumber && !pendingQueue.find(x => x == tipBlockNumber)) {
                await this.dispatchBlock(tipBlockNumber);
                Log.info(`blockchain sync: OK complete (block: ${tipBlockNumber}, transactions: ${pendingQueue.length})`);
            } else {
                Log.info(`blockchain sync: OK complete (transactions: ${pendingQueue.length})`);
            }
        } catch (exception) {
            Log.error('blockchain sync failed:', exception);
        }
    
        RPC.onNodeEvent = async (event) => {
            if (event.type == 'block' && typeof event.result.number == 'number' && event.result.number > 0) {
                await this.sync(event.result.number);
            }
        };
        this.syncing = false;
    }
    static async call(toAccount: string, method: string, args: any[]): Promise<any> {
        const asset = AssetId.fromHandle('BTC');
        const fromAccount = Signing.encodeAddress(new Pubkeyhash('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF')) || '';
        const receipt = await RPC.callTransaction(asset, fromAccount, toAccount, method, args);
        if (!receipt || !receipt.successful)
            throw new Error(`call ${toAccount}.${method} error`);
        return receipt.result;
    }
    static async getAccountBalances(account: string): Promise<{ asset: string, value: BigNumber }[]> {
        const balances = (await RPC.fetchAll((offset, count) => RPC.getAccountBalances(account, offset, count))) || [];
        return balances.map((v) => { 
            return {
                asset: v.asset.id,
                value: v.balance
            }
        });
    }
    static async getBlockchains(): Promise<BlockchainInfo[]> {
        if (this.blockchains != null)
            return this.blockchains;

        const results = await RPC.getBlockchains();
        if (!results)
            return [];

        return results.map((v) => {
            const asset = new AssetId(v.id);
            return {
                ...asset,
                divisibility: new BigNumber(v.divisibility),
                transactionFinality: new BigNumber(v.transaction_finality),
                transactionExpires: v.transaction_expires,
                compositionPolicy: v.composition_policy,
                tokenPolicy: v.token_policy,
                routingPolicy: v.routing_policy
            } as any
        });
    }
    static async getAssetHolders(asset: AssetId): Promise<number | null> {
        return RPC.getAssetHolders(asset, 0);
    }
    static async dispatchBlock(number: number): Promise<void> {
        const transactions = await RPC.getBlockTransactionsByNumber(number, 3);
        let accounts: string[] = [], matches = 0;
        let results: Record<string, EventInfo[]> = { };
        if (Array.isArray(transactions)) {
            for (let i = 0; i < transactions.length; i++) {
                const result = transactions[i];
                const receipt = result.receipt;
                if (!receipt.successful)
                    continue;

                if (result.affected && Array.isArray(result.affected.accounts))
                    accounts = accounts.concat(result.affected.accounts);

                const subtransactions: { transaction: any, sender: string, events: any[] }[] = [];
                if (result.transaction.type == 'rollup' && Array.isArray(result.transaction.transactions)) {
                    for (let j = 0; j < result.transaction.transactions.length; j++) {
                        const subtransaction = result.transaction.transactions[j];
                        const subhash = new Uint256(subtransaction.action.hash);
                        const eventsBegin = receipt.events.findIndex((v: any) => v.event == Types.Rollup && new Uint256(v.args[0]).eq(subhash));
                        if (eventsBegin != -1) {
                            const eventsEnd = receipt.events.findIndex((v: any, index: number) => index > eventsBegin && v.event == Types.Rollup);
                            subtransactions.push({
                                transaction: subtransaction.action,
                                sender: subtransaction.signer || receipt.from,
                                events: receipt.events.slice(eventsBegin, eventsEnd == -1 ? receipt.events.length : eventsEnd)
                            });
                        }
                    }
                } else {
                    subtransactions.push({
                        transaction: result.transaction,
                        sender: receipt.from,
                        events: receipt.events
                    });
                }

                for (let j = 0; j < subtransactions.length; j++) {
                    const target = subtransactions[j];
                    const toAccount = target.transaction.callable || '';
                    if (target.transaction.type != 'call' && target.transaction.type != 'deploy')
                        continue;
                    
                    const events = target.events.map((x) => ({
                        hash: target.transaction.hash || '',
                        fromAccount: target.sender || '',
                        toAccount: x.emitter || toAccount,
                        method: target.transaction.function || null,
                        args: target.transaction.args || [],
                        pays: target.transaction.pays || [],
                        event: {
                            type: BigNumber.isBigNumber(x.event) ? x.event.toNumber() : parseInt(x.event),
                            args: x.args
                        }
                    })).filter((x) => this.contracts.topics.indexOf(x.toAccount) != -1 && dispatchableEvents.indexOf(x.event.type) != -1);
                    for (let i = 0; i < events.length; i++) {
                        const event = events[i];
                        const subresults = results[event.toAccount];
                        if (subresults != null)
                            subresults.push(event);
                        else
                            results[event.toAccount] = [event];
                        ++matches;
                    }
                }
            }
        }

        const block = await RPC.getBlockByNumber(number);
        const blockTime = new BigNumber(block.generation_time);
        const blockDate = blockTime.isNaN() ? new Date() : new Date(blockTime.toNumber());
        const blockHash = new Uint256(block.hash, 16);
        const blockRef = { number: number, time: blockDate };
        await Exchange.setBlock({ blockNumber: number, blockHash: blockHash }, accounts);
        for (let account in results) {
            try {
                const accountRef = this.accountOf(account);
                if (accountRef != null) {
                    await Exchange.dispatchEvents(blockRef, accountRef, results[account]);
                }
            } catch (exception) {
                Log.error(`failed to dispatch block events (block: ${number}, account: ${account}):`, exception);
            }
        }
    }
    private static async findBlock(hash?: Uint256): Promise<any> {
        try {
            return await RPC.forcedPolicy('no-cache', () => RPC.getBlockByHash(hash?.toHex() || ''));
        } catch {
            return null;
        }
    }
}

export const dispatchableEvents: number[] = [
    Spot.DEX.Events.Config,
    Spot.DEX.Events.Order,
    Spot.DEX.Events.Pool,
    Spot.DEX.Events.Swap,
    Spot.DEX.Events.AssetTier,
    Spot.DLP.Events.Config,
    Spot.DLP.Events.PoolRefEvent
];