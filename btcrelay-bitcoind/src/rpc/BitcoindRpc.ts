import {BitcoindBlock, BitcoindBlockType} from "./BitcoindBlock";
import {BTCMerkleTree} from "./BTCMerkleTree";
import {BitcoinRpc, BtcBlockWithTxs, BtcSyncInfo, BtcTx} from "crosslightning-base";
import * as bitcoin from "bitcoinjs-lib";
import * as RpcClient from "@atomiqlabs/bitcoind-rpc";

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

type BitcoindBlockchainInfo = {
    chain : string,
    blocks : number,
    headers : number,
    bestblockhash : string,
    difficulty : number,
    time : number,
    mediantime : number,
    verificationprogress : number,
    initialblockdownload : boolean,
    chainwork : string,
    size_on_disk : number,
    pruned : boolean,
    pruneheight : number,
    automatic_pruning : boolean,
    prune_target_size : number,
    warnings : string
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

    async getTipHeight(): Promise<number> {

        const retrievedInfo = await new Promise<BitcoindBlockchainInfo>((resolve, reject) => {
            this.rpc.getBlockchainInfo((err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });

        return retrievedInfo.blocks;

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
                    if(err.code===-5) {
                        resolve(null);
                        return;
                    }
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });

        if(retrievedTx==null) return null;

        //Strip witness data
        const btcTx = bitcoin.Transaction.fromHex(retrievedTx.hex);

        for(let txIn of btcTx.ins) {
            txIn.witness = []; //Strip witness data
        }

        const resultHex = btcTx.toHex();

        retrievedTx.vout.forEach(e => {
            e.value = parseInt(e.value.toFixed(8).replace(new RegExp("\\.", 'g'), ""));
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
                vout.value = parseInt(vout.value.toFixed(8).replace(new RegExp("\\.", 'g'), ""));
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

    async getSyncInfo(): Promise<BtcSyncInfo> {
        const blockchainInfo = await new Promise<BitcoindBlockchainInfo>((resolve, reject) => {
            this.rpc.getBlockchainInfo((err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });

        return {
            ibd: blockchainInfo.initialblockdownload,
            verificationProgress: blockchainInfo.verificationprogress,
            headers: blockchainInfo.headers,
            blocks: blockchainInfo.blocks,
            _: blockchainInfo
        } as any;
    }

    async sendRawPackage(rawTxs: string[]): Promise<string[]> {
        const result = await new Promise<{package_msg: string, tx_results: {[wtxid: string]: {txid: string}}}>((resolve, reject) => {
            this.rpc.submitPackage(rawTxs, (err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });
        if(result.package_msg!=="success") throw new Error(
            result.package_msg+": "+Object.keys(result["tx-results"]).map(wtxid => result["tx-results"][wtxid].txid+"=\""+result["tx-results"][wtxid].error+"\"").join(", ")
        );
        return Object.keys(result["tx-results"]).map(wtxid => result["tx-results"][wtxid].txid);
    }

    sendRawTransaction(rawTx: string): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            this.rpc.sendRawTransaction(rawTx, (err, info) => {
                if(err) {
                    reject(err);
                    return;
                }
                resolve(info.result);
            });
        });
    }

}