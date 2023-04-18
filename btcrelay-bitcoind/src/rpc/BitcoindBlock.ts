import {BtcBlock} from "crosslightning-base";

export type BitcoindBlockType = {
    hash: string,
    confirmations: number,
    height: number,
    version: number,
    versionHex: string,
    merkleroot: string,
    time: number,
    mediantime: number,
    nonce: number,
    bits: string,
    difficulty: number,
    chainwork: string,
    nTx: number,
    previousblockhash: string,
    nextblockhash: string
}

export class BitcoindBlock implements BtcBlock {

    hash: string;
    confirmations: number;
    height: number;
    version: number;
    versionHex: string;
    merkleroot: string;
    time: number;
    mediantime: number;
    nonce: number;
    bits: string;
    difficulty: number;
    chainwork: string;
    nTx: number;
    previousblockhash: string;
    nextblockhash: string;

    constructor(obj: BitcoindBlockType) {
        this.hash = obj.hash;
        this.confirmations = obj.confirmations;
        this.height = obj.height;
        this.version = obj.version;
        this.versionHex = obj.versionHex;
        this.merkleroot = obj.merkleroot;
        this.time = obj.time;
        this.mediantime = obj.mediantime;
        this.nonce = obj.nonce;
        this.bits = obj.bits;
        this.difficulty = obj.difficulty;
        this.chainwork = obj.chainwork;
        this.nTx = obj.nTx;
        this.previousblockhash = obj.previousblockhash;
        this.nextblockhash = obj.nextblockhash;
    }

    getHeight(): number {
        return this.height;
    }

    getHash(): string {
        return this.hash;
    }

    getMerkleRoot(): string {
        return this.merkleroot;
    }

    getNbits(): number {
        return Buffer.from(this.bits, "hex").readUint32BE();
    }

    getNonce(): number {
        return this.nonce;
    }

    getPrevBlockhash(): string {
        return this.previousblockhash;
    }

    getTimestamp(): number {
        return this.time;
    }

    getVersion(): number {
        return this.version;
    }

}