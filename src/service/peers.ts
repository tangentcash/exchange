import { randomBytes } from 'crypto';
import { AssetId, ByteUtil } from 'tangentsdk';
import { WebSocket as PeerSocket } from '@fastify/websocket';
import BigNumber from 'bignumber.js';
import { Log } from '../logging';
import { Exchange } from './exchange';
import { Quotes } from './market';
import { OrderSide, Trade } from '../types';

export type PeerPrice = {
    asset: string,
    side: OrderSide,
    price: string,
    time: number
};

export type PeerBatch = {
    origin: string,
    time: number,
    base?: string,
    prices: PeerPrice[]
};

export class Peers {
    static id: string = ByteUtil.uint8ArrayToHexString(randomBytes(16));
    static sockets: Set<PeerSocket> = new Set<PeerSocket>();
    static seen: Record<string, number> = { };
    static clients: Set<string> = new Set<string>();
    static clientSockets: Record<string, WebSocket> = { };

    static attach(socket: PeerSocket): void {
        this.sockets.add(socket);
        socket.on('message', (message: Buffer) => {
            try {
                Peers.receive(socket, JSON.parse(message.toString('utf8')) as PeerBatch);
            } catch (exception) {
                Log.error('price peers: rejected malformed batch from peer:', exception);
                socket.close(4002, 'malformed');
            }
        });
        socket.on('close', () => this.sockets.delete(socket));
        socket.on('error', () => this.sockets.delete(socket));
        Log.info(`price peers: peer connected (peers: ${this.sockets.size})`);
    }

    static broadcast(trades: { asset: AssetId, trade: Omit<Trade, 'id' | 'pairId'> }[]): void {
        const base = Quotes.globalBase();
        if (base == null)
            return;

        const prices: PeerPrice[] = [];
        for (let i = 0; i < trades.length; i++) {
            prices.push({
                asset: trades[i].asset.id,
                side: trades[i].trade.side,
                price: trades[i].trade.price.toString(),
                time: trades[i].trade.time.getTime()
            });
        }
        this.send(null, { origin: this.id, time: Date.now(), base: base, prices: prices });
    }
    private static send(except: PeerSocket | null, batch: PeerBatch): void {
        if (this.sockets.size == 0)
            return;

        const message = JSON.stringify(batch);
        for (const socket of this.sockets) {
            if (socket == except)
                continue;
            try {
                socket.send(message);
            } catch {
                this.sockets.delete(socket);
            }
        }
    }

    static setupClients(urls: string[]): void {
        for (let i = 0; i < urls.length; i++) {
            const url = urls[i];
            if (typeof url != 'string' || this.clients.has(url))
                continue;
            this.clients.add(url);
            this.connect(url, 5000);
        }
    }
    private static connect(url: string, backoff: number): void {
        let delay = backoff;
        const open = () => {
            if (!this.clients.has(url))
                return;

            let socket: WebSocket;
            try {
                socket = new WebSocket(url);
            } catch (exception) {
                Log.error(`price peers: failed to connect to trusted peer (${url}):`, exception);
                setTimeout(open, delay);
                delay = Math.min(delay * 2, 60_000);
                return;
            }
            this.clientSockets[url] = socket;
            socket.onopen = () => {
                delay = backoff;
                Log.info(`price peers: connected to trusted peer (${url})`);
            };
            socket.onmessage = (event: MessageEvent) => {
                try {
                    Peers.receive(null, JSON.parse(typeof event.data == 'string' ? event.data : Buffer.from(event.data as ArrayBuffer).toString('utf8')) as PeerBatch);
                } catch (exception) {
                    Log.error(`price peers: trusted peer (${url}) message failed:`, exception);
                }
            };
            socket.onclose = (event: CloseEvent) => {
                if (this.clientSockets[url] == socket)
                    delete this.clientSockets[url];
                Log.info(`price peers: disconnected from trusted peer (${url}, code: ${event?.code ?? 'null'})`);
                if (this.clients.has(url)) {
                    setTimeout(open, delay);
                    delay = Math.min(delay * 2, 60_000);
                }
            };
            socket.onerror = () => { };
        };
        open();
    }

    private static receive(from: PeerSocket | null, batch: PeerBatch): void {
        try {
            if (batch == null || typeof batch.origin != 'string' || batch.origin == this.id || typeof batch.time != 'number' || !Array.isArray(batch.prices))
                return;
            if ((this.seen[batch.origin] ?? -1) >= batch.time)
                return;
            this.seen[batch.origin] = batch.time;

            // stream to all other connected peers (loop-safe: origin + timestamp dedup)
            this.send(from, batch);
            void Peers.applyPrices(batch);
        } catch (exception) {
            Log.error('price peers: off-chain price batch from peer failed:', exception);
        }
    }
    private static async applyPrices(batch: PeerBatch): Promise<void> {
        try {
            const base = Quotes.globalBase();
            if (base == null || batch.base != base) {
                Log.error(`price peers: ignoring off-chain prices from peer (${batch.origin}): base mismatch (${batch.base} != ${base})`);
                return;
            }

            const native = new AssetId().id;
            const trades: { asset: AssetId, trade: Omit<Trade, 'id' | 'pairId'> }[] = [];
            for (const price of batch.prices) {
                try {
                    const asset = new AssetId(price.asset);
                    if (asset.id == native)
                        continue;

                    trades.push({
                        asset: asset,
                        trade: {
                            side: price.side,
                            price: new BigNumber(price.price),
                            quantity: new BigNumber(0),
                            time: new Date(price.time)
                        }
                    });
                } catch (exception) {
                    Log.info(`price peers: ignoring malformed off-chain price (${price?.asset ?? 'null'}) from peer (${batch.origin}):`, exception);
                }
            }

            await Exchange.isolate((connection) => Exchange.setOracleTrades(trades, connection));
            Log.info(`price peers: applied ${trades.length}/${batch.prices.length} off-chain prices from peer (${batch.origin})`);
        } catch (exception) {
            Log.error(`price peers: off-chain price batch from peer (${batch.origin}) failed:`, exception);
        }
    }

    static async shutdown(): Promise<void> {
        this.clients.clear();
        for (const url in this.clientSockets) {
            try {
                this.clientSockets[url].close();
            } catch { }
        }
        this.clientSockets = { };

        for (const socket of this.sockets) {
            try {
                socket.close();
            } catch { }
        }
        this.sockets.clear();
    }
}
