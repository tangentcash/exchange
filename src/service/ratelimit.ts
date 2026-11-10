import { createHash, randomBytes } from 'crypto';
import { Log } from '../logging';

export type BookingLimits = {
    bookingsPerMinute: number,
    bookingsPerIpPerMinute: number
}

export type BookingAcquisition = {
    ok: boolean,
    scope: 'instance' | 'address' | null,
    retryAfter: number,
    time: number,
    key: string | null
}

export class BookingRateLimit {
    private static readonly WINDOW = 60_000;
    private static readonly salt = randomBytes(32);
    private static limits: BookingLimits = { bookingsPerMinute: 64, bookingsPerIpPerMinute: 1 };
    private static instance: number[] = [];
    private static addresses = new Map<string, number[]>();
    private static acquisitions = 0;

    static configure(partial: Partial<BookingLimits> | null | undefined): void {
        if (partial != null) {
            if (typeof partial.bookingsPerMinute == 'number' && partial.bookingsPerMinute > 0)
                this.limits.bookingsPerMinute = Math.floor(partial.bookingsPerMinute);
            if (typeof partial.bookingsPerIpPerMinute == 'number' && partial.bookingsPerIpPerMinute > 0)
                this.limits.bookingsPerIpPerMinute = Math.floor(partial.bookingsPerIpPerMinute);
        }
        Log.info('swap booking rate limits:', this.limits.bookingsPerMinute + '/min per instance,',
            this.limits.bookingsPerIpPerMinute + '/min per address (in-memory, addresses stored only as salted hashes)');
    }
    static get current(): BookingLimits {
        return { ...this.limits };
    }
    static acquireBooking(ip: string | null): BookingAcquisition {
        const now = Date.now();
        this.instance = this.prune(this.instance, now);
        if (this.instance.length >= this.limits.bookingsPerMinute)
            return { ok: false, scope: 'instance', retryAfter: this.retryAfter(this.instance, now), time: 0, key: null };

        const key = ip != null && ip.length > 0 ? this.fingerprint(ip) : null;
        const hits = key != null ? this.prune(this.addresses.get(key) || [], now) : [];
        if (key != null && hits.length >= this.limits.bookingsPerIpPerMinute) {
            this.addresses.set(key, hits);
            this.sweep(now);
            return { ok: false, scope: 'address', retryAfter: this.retryAfter(hits, now), time: 0, key: null };
        }
        this.sweep(now);
        return { ok: true, scope: null, retryAfter: 0, time: now, key: key };
    }
    static commitBooking(acquisition: BookingAcquisition): void {
        if (!acquisition.ok || acquisition.time == 0)
            return;
        this.instance.push(acquisition.time);
        if (acquisition.key != null) {
            const hits = this.addresses.get(acquisition.key) ?? [];
            hits.push(acquisition.time);
            this.addresses.set(acquisition.key, hits);
        }
    }
    private static retryAfter(hits: number[], now: number): number {
        if (hits.length == 0)
            return 1;
        return Math.max(1, Math.ceil((hits[0] + this.WINDOW - now) / 1000));
    }
    private static fingerprint(ip: string): string {
        return createHash('sha256').update(this.salt).update(ip).digest('hex');
    }
    private static prune(hits: number[], now: number): number[] {
        let first = 0;
        while (first < hits.length && hits[first] <= now - this.WINDOW)
            ++first;
        return first > 0 ? hits.slice(first) : hits;
    }
    private static sweep(now: number): void {
        if (++this.acquisitions % 1024 != 0)
            return;
        for (const [key, hits] of this.addresses) {
            const kept = this.prune(hits, now);
            if (kept.length == 0)
                this.addresses.delete(key);
            else if (kept !== hits)
                this.addresses.set(key, kept);
        }
    }
}
