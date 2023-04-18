import {BitcoindBlock, BitcoindBlockType} from "./BitcoindBlock";
import {BTCMerkleTree} from "./BTCMerkleTree";
import {BitcoinRpc, BtcBlockWithTxs, BtcTx} from "crosslightning-base";
import * as bitcoin from "bitcoinjs-lib";
import * as RpcClient from "bitcoind-rpc";

export type BitcoindVout = {
    value: number,
    n: number,
    scriptPubKey: {
        asm: string,
        hex: string,
        reqSigs: number,
        type: string,
        addresses: string[]
    }
};

export type BitcoindVin = {
    txid: string,
    vout: number,
    scriptSig: {
        asm: string,
        hex: string
    },
    sequence: number,
    txinwitness: string[]
};

export type BitcoindTransaction = {
    hex: string,
    txid: string,
    hash: string,
    size: number,
    vsize: number,
    weight: number,
    version: number,
    locktime: number,
    vin: BitcoindVin[],
    vout: BitcoindVout[],
    blockhash: string,
    confirmations: number,
    blocktime: number,
    time: number
};

type BitcoindRawBlock = {
    hash: string,
    confirmations: number,
    size: number,
    strippedsize: number,
    weight: number,
    height: number,
    version: number,
    versionHex: string,
    merkleroot: string,
    tx: BitcoindTransaction[],
    time: number,
    mediantime: number,
    nonce: number,
    bits: string,
    difficulty: number,
    nTx: number,
    previousblockhash: string,
    nextblockhash: string
}

export class BitcoindRpc implements BitcoinRpc<BitcoindBlock> {

    rpc: any;

    constructor(
        protocol: string,
        user: string,
        pass: string,
        host: string,
        port: number
    ) {
        this.rpc = new RpcClient({
            protocol,
            user,
            pass,
            host,
            port: port.toString()
        });
    }

    async getBlockHeader(blockhash: string): Promise<BitcoindBlock> {
        const retrievedHeader = await new Promise<BitcoindBlockType>((resolve, reject) => {
            this.rpc.getBlockHeader(blockhash, true, (err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });
        return new BitcoindBlock(retrievedHeader);
    }

    async isInMainChain(blockhash: string): Promise<boolean> {
        const retrievedHeader = await new Promise<BitcoindBlockType>((resolve, reject) => {
            this.rpc.getBlockHeader(blockhash, true, (err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });
        return retrievedHeader.confirmations>0;
    }

    async getMerkleProof(txId: string, blockhash: string): Promise<{
        reversedTxId: Buffer,
        pos: number,
        merkle: Buffer[],
        blockheight: number
    }> {
        return BTCMerkleTree.getTransactionMerkle(txId, blockhash, this.rpc);
    }

    async getTransaction(txId: string): Promise<BtcTx> {

        const retrievedTx = await new Promise<BitcoindTransaction>((resolve, reject) => {
            this.rpc.getRawTransaction(txId, 1, (err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });

        //Strip witness data
        const btcTx = bitcoin.Transaction.fromHex(retrievedTx.hex);

        for(let txIn of btcTx.ins) {
            txIn.witness = []; //Strip witness data
        }

        const resultHex = btcTx.toHex();

        retrievedTx.vout.forEach(e => {
            e.value = Math.floor(e.value*100000000);
        });

        return {
            blockhash: retrievedTx.blockhash,
            confirmations: retrievedTx.confirmations,
            txid: retrievedTx.txid,
            hex: resultHex,
            outs: retrievedTx.vout,
            ins: retrievedTx.vin
        }

    }

    getBlockhash(height: number): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            this.rpc.getBlockHash(height, (err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });
    }

    async getBlockWithTransactions(blockhash: string): Promise<BtcBlockWithTxs> {

        const block = await new Promise<BitcoindRawBlock>((resolve, reject) => {
            this.rpc.getBlock(blockhash, 2, (err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });

        block.tx.forEach(tx => {
            tx.vout.forEach(vout => {
                vout.value = Math.floor(vout.value*100000000);
            });
        });

        return {
            hash: block.hash,
            height: block.height,
            tx: block.tx.map(tx => {
                const btcTx = bitcoin.Transaction.fromHex(tx.hex);
                for(let txIn of btcTx.ins) {
                    txIn.witness = []; //Strip witness data
                }
                const resultHex = btcTx.toHex();

                return {
                    blockhash: tx.blockhash,
                    confirmations: tx.confirmations,
                    txid: tx.txid,
                    hex: resultHex,
                    outs: tx.vout,
                    ins: tx.vin
                };
            })
        };

    }

}