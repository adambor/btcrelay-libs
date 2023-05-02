
import {BtcRelay, BtcStoredHeader, RelaySynchronizer} from "crosslightning-base";
import {BitcoindBlock} from "../rpc/BitcoindBlock";
import {BitcoindRpc} from "../rpc/BitcoindRpc";

export class BtcRelaySynchronizer<V extends BtcStoredHeader<any>, T> implements RelaySynchronizer<V, T, BitcoindBlock> {

    btcRelay: BtcRelay<V,T,BitcoindBlock>;
    bitcoinRpc: BitcoindRpc;

    constructor(btcRelay: BtcRelay<V,T,BitcoindBlock>, bitcoinRpc: BitcoindRpc) {
        this.btcRelay = btcRelay;
        this.bitcoinRpc = bitcoinRpc;
    }

    async syncToLatestTxs(): Promise<{
        txs: T[]
        targetCommitedHeader: V,
        computedHeaderMap: {[blockheight: number]: V},
        blockHeaderMap: {[blockheight: number]: BitcoindBlock},
        btcRelayTipBlockHash: string,
        latestBlockHeader: BitcoindBlock
    }> {

        const tipData = await this.btcRelay.getTipData();

        let cacheData: {
            forkId: number,
            lastStoredHeader: V,
            tx: T,
            computedCommitedHeaders: V[]
        } = {
            forkId: 0,
            lastStoredHeader: null,
            tx: null,
            computedCommitedHeaders: null
        };

        const {resultStoredHeader, resultBitcoinHeader} = await this.btcRelay.retrieveLatestKnownBlockLog();
        cacheData.lastStoredHeader = resultStoredHeader;
        if(resultStoredHeader.getBlockheight()<tipData.blockheight) {
            cacheData.forkId = -1; //Indicate that we will be submitting blocks to fork
        }
        let spvTipBlockHeader: BitcoindBlock = resultBitcoinHeader;
        const btcRelayTipBlockHash: string = resultBitcoinHeader.hash;

        console.log("[BtcRelaySynchronizer]: Retrieved stored header with commitment: ", cacheData.lastStoredHeader);

        console.log("[BtcRelaySynchronizer]: SPV tip commit hash: ", tipData.commitHash);

        console.log("[BtcRelaySynchronizer]: SPV tip header: ", spvTipBlockHeader);

        const txsList: T[] = [];
        const blockHeaderMap: {[blockheight: number]: BitcoindBlock} = {
            [spvTipBlockHeader.height]: spvTipBlockHeader
        };
        const computedHeaderMap: {[blockheight: number]: V} = {};

        const saveHeaders = async (headerCache: BitcoindBlock[]) => {
            console.log("[BtcRelaySynchronizer]: Header cache: ", headerCache.map(e => e.hash));
            if(cacheData.forkId===-1) {
                cacheData = await this.btcRelay.saveNewForkHeaders(headerCache, cacheData.lastStoredHeader, tipData.chainWork)
            } else if(cacheData.forkId===0) {
                cacheData = await this.btcRelay.saveMainHeaders(headerCache, cacheData.lastStoredHeader);
            } else {
                cacheData = await this.btcRelay.saveForkHeaders(headerCache, cacheData.lastStoredHeader, cacheData.forkId, tipData.chainWork)
            }
            txsList.push(cacheData.tx);
            for(let storedHeader of cacheData.computedCommitedHeaders) {
                computedHeaderMap[storedHeader.getBlockheight()] = storedHeader;
            }
        };

        let headerCache: BitcoindBlock[] = [];
        while(spvTipBlockHeader.nextblockhash!=null) {

            const startTime = Date.now();

            const retrievedHeader = await this.bitcoinRpc.getBlockHeader(spvTipBlockHeader.nextblockhash);

            console.log("[BtcRelaySynchronizer]: Syncing blockheight (in "+(Date.now()-startTime)+"ms): ", retrievedHeader.height);

            blockHeaderMap[retrievedHeader.height] = retrievedHeader;
            headerCache.push(retrievedHeader);

            if(cacheData.forkId===0 ?
                headerCache.length>=this.btcRelay.maxHeadersPerTx :
                headerCache.length>=this.btcRelay.maxForkHeadersPerTx) {

                await saveHeaders(headerCache);

                headerCache = [];
            }

            spvTipBlockHeader = retrievedHeader;

            // if(retrievedHeader.nextblockhash!=null) {
            //     await new Promise((resolve) => setTimeout(resolve, 1000));
            // }
        }

        if(headerCache.length>0) {
            await saveHeaders(headerCache);
        }

        return {
            txs: txsList,
            targetCommitedHeader: cacheData.lastStoredHeader,
            blockHeaderMap,
            computedHeaderMap,
            btcRelayTipBlockHash,

            latestBlockHeader: spvTipBlockHeader
        };

    }

}
