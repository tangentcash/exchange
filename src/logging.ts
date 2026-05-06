import fs from 'fs';
import util from 'util';
import { Common } from './common';

export type Options = {
    infoPath: string,
    errorPath: string,
    queryPath: string
};

export class Log
{
    static infoStream: fs.WriteStream | null;
    static errorStream: fs.WriteStream | null;
    static queryStream: fs.WriteStream | null;

    static setup(config: Options): void {
        this.infoStream = (config.infoPath ? fs.createWriteStream(Common.patch(config.infoPath) || config.infoPath, { flags: 'a' }) : null);
        this.errorStream = (config.errorPath ? fs.createWriteStream(Common.patch(config.errorPath) || config.errorPath, { flags: 'a' }) : null);
        this.queryStream = (config.queryPath ? fs.createWriteStream(Common.patch(config.queryPath) || config.queryPath, { flags: 'a' }) : null);
    }
    static info(...args: unknown[]): void {
        if (!args.length)
            return;

        let message = [this.getDate() + ' info/' + Common.source(2), ...args];
        if (this.infoStream != null)
            this.infoStream.write(util.format.apply(null, message) + '\n');
            
        console.log.apply(console, message);
    }
    static error(...args: unknown[]): void {
        if (!args.length)
            return;

        let message = [this.getDate() + ' error/' + Common.source(2), ...args];
        if (this.errorStream != null)
            this.errorStream.write(util.format.apply(null, message) + '\n');
            
        console.error.apply(console, message);
    }
    static query(...args: unknown[]): void {
        if (!args.length)
            return;

        let message = [this.getDate() + ' query/' + Common.source(2), ...args];
        if (this.queryStream != null)
            this.queryStream.write(util.format.apply(null, message) + '\n');
    }
    static getDate(): string {
        let time = new Date();
        let day = time.getDate();
        let month = time.getMonth() + 1;
        let year = time.getFullYear();
        let hour = time.getHours();
        let minute = time.getMinutes();
        let second = time.getSeconds();
        return (day < 10 ? '0' + day : day) + '.' + (month < 10 ? '0' + month : month) + '.' +  year + 'T' + (hour < 10 ? '0' + hour : hour) + ':' + (minute < 10 ? '0' + minute : minute) + ':' + (second < 10 ? '0' + second : second) + 'Z';
    }
}
