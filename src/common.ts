import path from 'path';
import fs from 'fs';
import util from 'util';
import BigNumber from 'bignumber.js';
import { Uint256 } from 'tangentsdk';

export class Common {
    static num(value: any): number | undefined {
        switch (typeof value) {
            case 'bigint':
            case 'string':
            case 'number':
                return new BigNumber(value).toNumber();
            default:
                if (value instanceof Uint256)
                    return new BigNumber(value.toString()).toNumber();
                if (BigNumber.isBigNumber(value))
                    return value.toNumber();
                return undefined;
        }
    }
    static bn(value: any): BigNumber | undefined {
        switch (typeof value) {
            case 'bigint':
            case 'string':
            case 'number':
                return new BigNumber(value);
            default:
                if (value instanceof Uint256)
                    return new BigNumber(value.toString());
                if (BigNumber.isBigNumber(value))
                    return value;
                return undefined;
        }
    }
    static u256(value: any): Uint256 | undefined {
        switch (typeof value) {
            case 'bigint':
                return new Uint256(value.toString());
            case 'string':
            case 'number':
                return new Uint256(value);
            default:
                if (value instanceof Uint8Array)
                    return new Uint256(value);
                if (Buffer.isBuffer(value))
                    return new Uint256(new Uint8Array(value));
                if (BigNumber.isBigNumber(value))
                    return new Uint256(value.toFixed(0));
                if (value instanceof Uint256)
                    return value;
                return undefined;
        }
    }
    static source(skips?: number): any {    
        const ref: any = util;
        const trace = (ref.getCallSite ? ref.getCallSite : ref.getCallSites)((skips || 0) + 1);
        const frame = trace[trace.length - 1];
        return path.basename(frame.scriptName) + ':' + frame.lineNumber;
    }
    static patch(targetPath: string | null): string | null {
        if (!targetPath)
            return null;
        
        let base = path.join(process.cwd(), targetPath);
        let result = base;   
        try {
            if (fs.lstatSync(result).isFile())
                result = path.dirname(result) || result;
        } catch {
            if (result.substring(result.lastIndexOf('/') + 1).indexOf('.') !== -1)
                result = path.dirname(result) || result;
        }
    
        if (!fs.existsSync(result))
            fs.mkdirSync(result, { recursive: true });
    
        return base;
    }
    static args(baseline?: Record<string, any>): Record<string, any> {
        let result: Record<string, any> = { };
        process.argv.forEach((v) => {
            if (v.startsWith('--')) {
                const valueIndex = v.indexOf('=');
                const keys = v.substring(2, valueIndex != -1 ? valueIndex : undefined).split('.');
                let value = valueIndex != -1 ? v.substring(valueIndex + 1).trim() : 'true';
                let target = result;
                for (let i = 0; i < keys.length - 1; i++) {
                    const key = keys[i];
                    if (!target[key])
                        target[key] = { };
                    target = target[key];
                }
                try {
                    target[keys[keys.length - 1]] = JSON.parse(value);
                } catch {
                    target[keys[keys.length - 1]] = value;
                }
            }
        });
        return baseline ? { ...baseline, ...result } : result;
    }
}