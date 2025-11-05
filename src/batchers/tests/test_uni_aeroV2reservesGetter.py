import json
import os

from eth_abi import decode, encode
from web3 import Web3

w3 = Web3(
    Web3.HTTPProvider(
        "https://base-mainnet.g.alchemy.com/v2/9tUX8QBj5DftD8imSlMpHUdRvmBY4yy-"
    )
)

with open(
    os.path.join(
        f"itrc_chain_data/itrc_contracts/{os.getenv('CHAIN_ID')}",
        "UniswapAeroV2ReservesGetter.json",
    ),
    "r",
) as f:
    bytecode = json.loads(f.read())["bytecode"]["object"]

# Convert integer addresses to hex strings
pairs = [
    Web3.to_checksum_address("0xCc129Dd137d6Bb820a0E8f01C092B0e69fe7e2c0"),
    Web3.to_checksum_address("0x050e91B3ABe34281285084cBB63f3A42d0dE2c21"),
]

input_data = encode(["address[]"], [pairs])  # Note: pairs needs to be in a list
input_data = bytecode + input_data.hex()
encoded_return_data = w3.eth.call({"data": input_data})
print(encoded_return_data.hex())
block_number, decoded_return_data = decode(
    ["uint256", "bytes32[2][]"], encoded_return_data
)
result = decoded_return_data
for i in range(len(pairs)):
    print(f"{pairs[i]} reserve0 : {result[i][0].hex()}")
    print(f"{pairs[i]} reserve1 : {result[i][1].hex()}")


# 00000000000000000000000000000000000000000000000000000000015f230d
# 0000000000000000000000000000000000000000000000000000000000000040
# 0000000000000000000000000000000000000000000000000000000000000002
# 0000000000000000000000000000000000000000000000000000000012738444
# 000000000000000000000000000000000000000000000000000002b829e34c65
# 0000000000000000000000000000000000000000000002b9f952dfad5e7acec3
# 0000000000000000000000000000000000000000000000000002c262789c6469
