// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

// Uniswap V3 Pool interface
interface IUniswapV3Pool {
    function slot0() external view returns (
        uint160 sqrtPriceX96,
        int24 tick,
        uint16 observationIndex,
        uint16 observationCardinality,
        uint16 observationCardinalityNext,
        uint8 feeProtocol,
        bool unlocked
    );

    function ticks(int24 tick) external view returns (
        uint128 liquidityGross,
        int128 liquidityNet,
        uint256 feeGrowthOutside0X128,
        uint256 feeGrowthOutside1X128,
        int56 tickCumulativeOutside,
        uint160 secondsPerLiquidityOutsideX128,
        uint32 secondsOutside,
        bool initialized
    );

    function tickBitmap(int16 wordPosition) external view returns (uint256);
}

// Contract for batch fetching Uniswap V3 tick data
contract UniswapV3TickGetter {
    struct TickDataRequest {
        address poolAddress;
        int24[] ticks;
    }

    constructor(TickDataRequest[] memory requests) {
        bytes32[][] memory tickData = new bytes32[][](requests.length);

        for (uint256 i = 0; i < requests.length; i++) {
            IUniswapV3Pool pool = IUniswapV3Pool(requests[i].poolAddress);
            tickData[i] = new bytes32[](requests[i].ticks.length);

            for (uint256 j = 0; j < requests[i].ticks.length; j++) {
                (
                    uint128 liquidityGross,
                    int128 liquidityNet,
                    , , , , ,  // Skip other tick data fields
                ) = pool.ticks(requests[i].ticks[j]);

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

// Contract for batch fetching Uniswap V3 tick bitmap data
contract UniswapV3TickBitmapGetter {
    struct BitmapRequest {
        address poolAddress;
        int16[] wordPositions;
    }

    constructor(BitmapRequest[] memory requests) {
        uint256[][] memory bitmaps = new uint256[][](requests.length);

        for (uint256 i = 0; i < requests.length; i++) {
            IUniswapV3Pool pool = IUniswapV3Pool(requests[i].poolAddress);
            bitmaps[i] = new uint256[](requests[i].wordPositions.length);

            for (uint256 j = 0; j < requests[i].wordPositions.length; j++) {
                bitmaps[i][j] = pool.tickBitmap(requests[i].wordPositions[j]);
            }
        }

        // Return block number and bitmap data
        bytes memory result = abi.encode(block.number, bitmaps);
        assembly {
            return(add(result, 0x20), mload(result))
        }
    }
}