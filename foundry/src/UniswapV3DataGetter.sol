// SPDX-License-Identifier:MIT

pragma solidity 0.8.28;

interface PancakeSwapV3Pool {
    function slot0()
        external
        view
        returns (uint160, int24, uint16, uint16, uint16, uint32, bool);

    function factory() external view returns (address);
}

interface UniswapV3Pool {
    function slot0()
        external
        view
        returns (uint160, int24, uint16, uint16, uint16, uint8, bool);

    function liquidity() external view returns (uint128);

    function factory() external view returns (address);
}

contract Uniswap_PancakeV3DataGetter {
    address immutable PANCAKESWAPFACTORY =
        0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865;
    address immutable UNISWAPFACTORY =
        0x1F98431c8aD98523631AE4a59f267346ea31F984;

    constructor(address[] memory pools) {
        uint256 pairs_length = pools.length;
        bytes32[2][] memory reserveData = new bytes32[2][](pairs_length);
        uint256 bt;

        for (uint256 i = 0; i < pairs_length; i++) {
            uint128 liquidity = UniswapV3Pool(pools[i]).liquidity();
            reserveData[i][0] = bytes32(uint256(liquidity));
            if (UniswapV3Pool(pools[i]).factory() == PANCAKESWAPFACTORY) {
                (uint160 sPX96, int24 tick, , , , , ) = PancakeSwapV3Pool(pools[i])
                    .slot0();
                reserveData[i][1] = bytes32(abi.encodePacked(sPX96, tick));
            } else {
                (uint160 sPX96, int24 tick, , , , , ) = UniswapV3Pool(pools[i])
                    .slot0();
                reserveData[i][1] = bytes32(abi.encodePacked(sPX96, tick));
            }
        }
        assembly {
            bt := number()
        }
        bytes memory data = abi.encode(bt, reserveData);
        assembly {
            return(add(data, 0x20), mload(data))
        }
    }
}