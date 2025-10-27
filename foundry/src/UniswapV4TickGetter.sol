// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

// StateView interface based on the actual contract
interface IStateView {
    function getSlot0(bytes32 poolId) external view returns (
        uint160 sqrtPriceX96,
        int24 tick,
        uint24 protocolFee,
        uint24 lpFee
    );

    function getLiquidity(bytes32 poolId) external view returns (uint128 liquidity);


    function getTickBitmap(bytes32 poolId, int16 wordPos) external view returns (uint256);

    function getTickLiquidity(bytes32 poolId, int24 tick) external view returns (
        uint128 liquidityGross,
        int128 liquidityNet
    );

}

// Contract for batch fetching Uniswap V4 tick data via StateView
contract UniswapV4TickGetter {
    IStateView public constant STATE_VIEW = IStateView(0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227);

    struct TickDataRequest {
        bytes32 poolId;
        int24[] ticks;
    }

    constructor(TickDataRequest[] memory requests) {
        bytes32[][] memory tickData = new bytes32[][](requests.length);

        for (uint256 i = 0; i < requests.length; i++) {
            tickData[i] = new bytes32[](requests[i].ticks.length);

            for (uint256 j = 0; j < requests[i].ticks.length; j++) {
                (
                    uint128 liquidityGross,
                    int128 liquidityNet
                ) = STATE_VIEW.getTickLiquidity(requests[i].poolId, requests[i].ticks[j]);

                tickData[i][j] = bytes32(abi.encodePacked(liquidityGross, liquidityNet));
            }
        }

        // Return block number and tick data
        bytes memory result = abi.encode(block.number, tickData);
        assembly {
            return(add(result, 0x20), mload(result))
        }
    }
}

// Separate contract for tick bitmap data (using storage slots directly)
contract UniswapV4TickBitmapGetter {
    IStateView public constant STATE_VIEW = IStateView(0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227);

    struct BitmapRequest {
        bytes32 poolId;
        int16[] wordPositions;
    }

    constructor(BitmapRequest[] memory requests) {
        uint256[][] memory bitmaps = new uint256[][](requests.length);

        for (uint256 i = 0; i < requests.length; i++) {
            bitmaps[i] = new uint256[](requests[i].wordPositions.length);

            for (uint256 j = 0; j < requests[i].wordPositions.length; j++) {
                bitmaps[i][j] = STATE_VIEW.getTickBitmap(requests[i].poolId, requests[i].wordPositions[j]);
            
            }
        }

        // Return block number and bitmap data
        bytes memory result = abi.encode(block.number, bitmaps);
        assembly {
            return(add(result, 0x20), mload(result))
        }
    }
}