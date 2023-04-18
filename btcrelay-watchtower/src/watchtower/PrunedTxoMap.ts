import {createHash} from "crypto";
import * as fs from "fs/promises";
import {BitcoinRpc, BtcBlock, BtcBlockWithTxs} from "crosslightning-base";


export class PrunedTxoMap {

    readonly map = new Map<string, {
        txId: string,
        vout: number,
        height: number
    }>();
    readonly blocksMap = new Map<number, {
        txoHashes: Buffer[],
        blockHash: string
    }>();

    readonly filename: string;
    tipHeight: number;

    readonly bitcoinRpc: BitcoinRpc<any>;

    readonly pruningFactor: number;

    constructor(filename: string, bitcoinRpc: BitcoinRpc<BtcBlock>, pruningFactor?: number) {
        this.filename = filename;
        this.bitcoinRpc = bitcoinRpc;
        this.pruningFactor = pruningFactor || 30;
    }

    async init(btcRelayHeight: number): Promise<number> {

        //Load last synced blockheight
        try {
            const result = await fs.readFile(this.filename);
            const height = parseInt(result.toString());
            btcRelayHeight = height;
        } catch (e) {}

        this.tipHeight = btcRelayHeight;

        //Build up the index for the last synced blockheight
        for(let i=0;i<this.pruningFactor;i++) {
            const blockHash = await this.bitcoinRpc.getBlockhash(btcRelayHeight-i);

            const {block} = await this.addBlock(blockHash, null, true);
        }

        return this.tipHeight;

    }

    async syncToTipHash(tipBlockHash: string, waitingForTxosMap?: Map<string, any>): Promise<Map<string, {
        txId: string,
        vout: number,
        height: number
    }>> {
        console.log("[PrunedTxoMap]: Syncing to tip hash: ", tipBlockHash);

        const blockHashes = [tipBlockHash];
        while(true) {
            const btcBlockHeader = await this.bitcoinRpc.getBlockHeader(blockHashes[blockHashes.length-1]);
            const previousHeight = btcBlockHeader.getHeight()-1;
            const previousHash = btcBlockHeader.getPrevBlockhash();
            const data = this.blocksMap.get(previousHeight);

            //Correct block already in cache
            if(data!=null) {
                if(data.blockHash===previousHash) break;
            }

            //Will replace all the existing cache anyway
            const minBlockHeight = this.tipHeight-this.pruningFactor;
            if(btcBlockHeader.getHeight()<minBlockHeight) {
                break;
            }

            blockHashes.push(previousHash);
        }

        const totalFoundTxos = new Map<string, {
            txId: string,
            vout: number,
            height: number
        }>();

        console.log("[PrunedTxoMap]: Syncing through blockhashes: ", blockHashes);

        for(let i=blockHashes.length-1;i>=0;i--) {
            const {foundTxos} = await this.addBlock(blockHashes[i], waitingForTxosMap);
            foundTxos.forEach((value, key: string, map) => {
                totalFoundTxos.set(key, value);
            })
        }

        return totalFoundTxos;

    }

    static toTxoHash(value: number, outputScript: string): Buffer {
        const buff = Buffer.alloc((outputScript.length/2) + 8);
        buff.writeBigUInt64LE(BigInt(value));
        buff.write(outputScript, 8, "hex");
        return createHash("sha256").update(buff).digest();
    }

    async addBlock(headerHash: string, waitingForTxosMap?: Map<string, any>, noSaveTipHeight?: boolean): Promise<{
        block: BtcBlockWithTxs,
        foundTxos: Map<string, {
            txId: string,
            vout: number,
            height: number
        }>
    }> {

        const block: BtcBlockWithTxs = await this.bitcoinRpc.getBlockWithTransactions(headerHash);

        console.log("[PrunedTxoMap]: Adding block  "+block.height+", hash: ", block.hash);
        if(!noSaveTipHeight) {
            this.tipHeight = block.height;
            await fs.writeFile(this.filename, this.tipHeight.toString());
        }

        const foundTxos = new Map<string, {
            txId: string,
            vout: number,
            height: number
        }>();

        const blockTxoHashes: Buffer[] = [];

        if(this.blocksMap.has(block.height)) {
            console.log("[PrunedTxoMap]: Fork block hash: ", block.hash);
            //Forked off
            for(let txoHash of this.blocksMap.get(block.height).txoHashes) {
                this.map.delete(txoHash.toString("hex"));
            }
        }

        for(let tx of block.tx) {
            for(let vout of tx.outs) {
                const txoHash = PrunedTxoMap.toTxoHash(vout.value, vout.scriptPubKey.hex);
                blockTxoHashes.push(txoHash);
                const txObj = {
                    txId: tx.txid,
                    vout: vout.n,
                    height: block.height
                };
                const txoHashHex = txoHash.toString("hex");
                this.map.set(txoHashHex, txObj);
                if(waitingForTxosMap!=null && waitingForTxosMap.has(txoHashHex)) {
                    foundTxos.set(txoHashHex, txObj);
                }
            }
        }

        this.blocksMap.set(block.height, {
            txoHashes: blockTxoHashes,
            blockHash: block.hash
        });

        //Pruned
        if(this.blocksMap.has(block.height-this.pruningFactor)) {
            console.log("[PrunedTxoMap]: Pruning block height: ", block.height-this.pruningFactor);
            //Forked off
            for(let txoHash of this.blocksMap.get(block.height-this.pruningFactor).txoHashes) {
                this.map.delete(txoHash.toString("hex"));
            }
            this.blocksMap.delete(block.height-this.pruningFactor);
        }

        return {
            block,
            foundTxos
        };

    }

    getTxoObject(txoHash: string): {
        txId: string,
        vout: number,
        height: number
    } {
        return this.map.get(txoHash);
    }

}
