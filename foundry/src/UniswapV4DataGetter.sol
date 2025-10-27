// SPDX-License-Identifier:MIT

pragma solidity 0.8.30;

interface UniswapV4StateView {
    function getSlot0(bytes32 poolID) external view returns (uint160, int24, uint24, uint24);
    function getLiquidity(bytes32 poolID) external view returns (uint128);
    
}


contract UniswapV4DataGetter {
    address immutable STATEVIEW_CONTRACT = 0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227;

    constructor(bytes32[] memory poolIDs) {
        uint256 poolIDs_length = poolIDs.length;
        bytes32[2][] memory reserveData = new bytes32[2][](poolIDs_length);
        uint256 bt;

        for (uint256 i = 0; i < poolIDs_length; i++) {
            uint128 liquidity = UniswapV4StateView(STATEVIEW_CONTRACT).getLiquidity(poolIDs[i]);
            (uint160 sPX96, int24 tick, ,) = UniswapV4StateView(STATEVIEW_CONTRACT).getSlot0(poolIDs[i]);
            reserveData[i][0] = bytes32(uint256(liquidity));
            reserveData[i][1] = bytes32(abi.encodePacked(sPX96, tick));
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