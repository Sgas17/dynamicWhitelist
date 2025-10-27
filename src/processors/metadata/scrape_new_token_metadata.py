import requests
import json
import os
import sys
import dotenv
from sqlalchemy import create_engine, text
import time
from typing import Dict, List, Set, Optional, Any
import logging
from pathlib import Path

from web3 import Web3
from eth_abi.abi import encode, decode
from eth_utils.address import to_checksum_address

# Load environment variables
dotenv.load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CoinGeckoTokenComparator:
    def __init__(self):
        self.ethereum_tokens = self.load_ethereum_tokens()
        self.coingecko_tokens = {}
        self.missing_tokens = set()
        self.new_token_data = []
        self.web3 = Web3(Web3.HTTPProvider(os.getenv("RPC_URL")))

        # Database connection
        self.db_uri = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        self.engine = create_engine(self.db_uri)

    def load_ethereum_tokens(self) -> Set[str]:
        """Load ethereum unique tokens from JSON file"""
        try:
            with open(Path.home() / "dynamic_whitelist" / "data" / "ethereum_unique_tokens.json", "r") as f:
                tokens = json.load(f)
                tokens.pop(-1)
            logger.info(f"Loaded {len(tokens)} Ethereum tokens")
            return set(token.lower() for token in tokens)
        except FileNotFoundError:
            logger.error("ethereum_unique_tokens.json not found")
            return set()

    def load_coingecko_tokens_from_db(self):
        """Load existing CoinGecko tokens from database"""
        sql = """
        SELECT DISTINCT
            cgt.id,
            cgt.name,
            cgt.symbol,
            cgtp.platform,
            cgtp.address
        FROM
            coingecko_tokens cgt
        JOIN coingecko_token_platforms cgtp ON cgt.id = cgtp.token_id
        WHERE cgtp.platform = 'ethereum';
        """

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                ethereum_addresses = set()
                for row in result.mappings():
                    if row["address"]:
                        ethereum_addresses.add(row["address"].lower())

                logger.info(
                    f"Loaded {len(ethereum_addresses)} Ethereum addresses from CoinGecko DB"
                )
                return ethereum_addresses
        except Exception as e:
            logger.error(f"Error loading CoinGecko tokens from DB: {e}")
            return set()


    def load_missing_jared_tokens_from_db(self, platform: str = "ethereum"):
        """Load missing tokens from database"""
        platform_table_name = f"missing_coingecko_tokens_{platform}"

        sql = f"""
        SELECT DISTINCT address
        FROM {platform_table_name};
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                missing_tokens = set()
                for row in result.mappings():
                    missing_tokens.add(row["address"].lower())
                logger.info(f"Loaded {len(missing_tokens)} missing tokens from {platform_table_name}")
                return missing_tokens
        except Exception as e:
            logger.warning(f"Could not load missing tokens from DB (table may not exist yet): {e}")
            return set()

    def find_missing_tokens(self, platform: str = "ethereum"):
        """Find tokens that are in ethereum_unique_tokens.json but not in CoinGecko DB"""
        coingecko_addresses = self.load_coingecko_tokens_from_db()
        jared_tokens = self.load_missing_jared_tokens_from_db(platform=platform)

        self.missing_tokens = self.ethereum_tokens - coingecko_addresses - jared_tokens
        logger.info(f"Found {len(self.missing_tokens)} missing tokens")
        return self.missing_tokens

    def create_missing_tokens_table(self, platform: str = "ethereum"):
        """Create missing tokens tables matching existing database schema"""
        
        # Create platforms table (matching coingecko_token_platforms)
        platforms_table_name = "missing_coingecko_tokens_platforms"
        
        # Create platform-specific metadata table (matching network_1_erc20_metadata)
        platform_table_name = f"missing_coingecko_tokens_{platform}"

        # Create platforms table SQL - matches coingecko_token_platforms structure
        create_platforms_sql = f"""
        CREATE TABLE IF NOT EXISTS {platforms_table_name} (
            address VARCHAR(255) NOT NULL,
            token_id VARCHAR(255) NOT NULL,
            decimals INTEGER,
            platform VARCHAR(255),
            total_supply NUMERIC(78, 0) DEFAULT 0,
            PRIMARY KEY (address, token_id)
        );
        """

        # Create platform-specific table SQL - matches network_1_erc20_metadata structure  
        create_platform_sql = f"""
        CREATE TABLE IF NOT EXISTS {platform_table_name} (
            address VARCHAR(42) PRIMARY KEY,
            symbol VARCHAR(50),
            decimals INTEGER,
            name VARCHAR(255)
        );
        """

        try:
            with self.engine.connect() as conn:
                # Create platforms table
                conn.execute(text(create_platforms_sql))
                logger.info(f"Created/verified platforms table: {platforms_table_name}")
                
                # Create platform-specific metadata table
                conn.execute(text(create_platform_sql))
                logger.info(f"Created/verified platform table: {platform_table_name}")

                # Check and migrate existing tables if needed
                self._migrate_table_schema_if_needed(conn, platforms_table_name)
                
                conn.commit()
                logger.info("Missing tokens tables setup completed successfully")
                
        except Exception as e:
            logger.error(f"Error creating/updating tables: {e}")
            raise

    def _migrate_table_schema_if_needed(self, conn, table_name: str):
        """Migrate table schema if columns need updates"""
        try:
            # Check if total_supply column exists and has correct type
            check_column_sql = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = :table_name 
            AND column_name = 'total_supply'
            """
            result = conn.execute(text(check_column_sql), {'table_name': table_name}).fetchone()
            
            if result and result[1] != 'numeric':
                # Column exists but wrong type, alter it
                logger.info(f"Migrating total_supply column type in {table_name}")
                alter_sql = f"""
                ALTER TABLE {table_name} 
                ALTER COLUMN total_supply TYPE NUMERIC(78, 0) USING total_supply::NUMERIC(78, 0)
                """
                conn.execute(text(alter_sql))
                
        except Exception as e:
            logger.warning(f"Schema migration warning for {table_name}: {e}")
            


    def get_token_info_by_address(self, address: str) -> Optional[Dict[str, Any]]:
        """Get token information from CoinGecko API by contract address"""
        try:
            # Use CoinGecko's contract address endpoint
            url = f"https://api.coingecko.com/api/v3/coins/ethereum/contract/{address}"

            headers = {
                "accept": "application/json",
                "x-cg-demo-api-key": os.getenv(
                    "COINGECKO_API_KEY", ""
                ),  # Optional API key
            }

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()
                return data
            elif response.status_code == 404:
                logger.warning(f"Token not found on CoinGecko: {address}")
                return None
            elif response.status_code == 429:
                logger.warning("Rate limited, waiting...")
                time.sleep(60)  # Wait 1 minute for rate limit
                return self.get_token_info_by_address(address)  # Retry
            else:
                logger.error(
                    f"API error {response.status_code} for {address}: {response.text}"
                )
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {address}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {address}: {e}")
            return None

    def scrape_missing_token_data(self, limit: Optional[int] = None):
        """Scrape data for missing tokens from CoinGecko API"""
        if not self.missing_tokens:
            logger.info("No missing tokens to scrape")
            return

        tokens_to_process = list(self.missing_tokens)
        if limit:
            tokens_to_process = tokens_to_process[:limit]

        logger.info(f"Starting to scrape {len(tokens_to_process)} tokens")

        for i, address in enumerate(tokens_to_process, 1):
            logger.info(f"Processing token {i}/{len(tokens_to_process)}: {address}")

            token_data = self.get_token_info_by_address(address)

            if token_data:
                try:
                    # Extract relevant information
                    processed_token = self.process_token_data(token_data, address)
                    if processed_token:
                        self.new_token_data.append(processed_token)
                        logger.info(
                            f"Successfully processed: {processed_token['name']} ({processed_token['symbol']})"
                        )
                except Exception as e:
                    logger.error(f"Error processing token data for {address}: {e}")

            # Rate limiting - be respectful to CoinGecko API
            if i % 10 == 0:  # Every 10 requests
                logger.info("Rate limiting pause...")
                time.sleep(
                    12
                )  # 12 second pause (5 requests per minute max for free tier)
            else:
                time.sleep(1.2)  # 1.2 second between requests

        logger.info(
            f"Completed scraping. Found data for {len(self.new_token_data)} tokens"
        )

    def process_token_data(
        self, token_data: Dict[str, Any], address: str
    ) -> Optional[Dict[str, Any]]:
        """Process raw CoinGecko token data into our format"""
        try:
            # Extract basic information
            token_info = {
                "id": token_data.get("id", f"unknown-{address}"),
                "name": token_data.get("name", "Unknown Token"),
                "symbol": token_data.get("symbol", "").upper(),
                "market_cap_rank": token_data.get("market_cap_rank", None) or None,
                "address": address.lower(),
                "platforms": {},
            }

            # Extract platform information
            platforms = token_data.get("platforms", {})
            if platforms:
                for platform, platform_address in platforms.items():
                    if platform_address:
                        token_info["platforms"][platform] = platform_address.lower()

            # If no platforms found, add ethereum as platform
            if not token_info["platforms"] and address:
                token_info["platforms"]["ethereum"] = address.lower()

            # Extract contract details if available
            contract_data = token_data.get("detail_platforms", {})
            if contract_data:
                ethereum_contract = contract_data.get("ethereum", {})
                if ethereum_contract:
                    token_info["decimals"] = ethereum_contract.get("decimal_place", 0)
                    token_info["contract_address"] = ethereum_contract.get(
                        "contract_address", address
                    ).lower()
                else:
                    token_info["decimals"] = None
            else:
                token_info["decimals"] = None

            return token_info

        except Exception as e:
            logger.error(f"Error processing token data: {e}")
            return None

    def insert_token_data(self, token_data: Optional[Dict[str, Any]] = None):
        """Insert scraped token data into the missing tokens tables"""
        if not self.new_token_data:
            if token_data is None:
                logger.info("No new token data to insert")
                return
            else:  # If token data is provided, insert it
                self.new_token_data.append(token_data)

        logger.info(f"Inserting {len(self.new_token_data)} tokens into database")

        try:
            with self.engine.connect() as conn:
                for token in self.new_token_data:
                    # Insert platform data into platform-specific metadata tables (network_1_erc20_metadata style)
                    for platform, platform_address in token["platforms"].items():
                        platform_table_name = f"missing_coingecko_tokens_{platform}"
                        platform_sql = f"""
                        INSERT INTO {platform_table_name} 
                        (address, symbol, decimals, name)
                        VALUES (:address, :symbol, :decimals, :name)
                        ON CONFLICT (address) DO UPDATE SET
                            symbol = EXCLUDED.symbol,
                            decimals = EXCLUDED.decimals,
                            name = EXCLUDED.name;
                        """

                        conn.execute(
                            text(platform_sql),
                            {
                                "address": platform_address,
                                "symbol": token.get("symbol", "UNK"),
                                "decimals": token.get("decimals", 0),
                                "name": token.get("name", "Unknown Token"),
                            },
                        )

                        # Insert into platforms table (coingecko_token_platforms style)
                        platforms_sql = """
                        INSERT INTO missing_coingecko_tokens_platforms 
                        (address, token_id, decimals, platform, total_supply)
                        VALUES (:address, :token_id, :decimals, :platform, :total_supply)
                        ON CONFLICT (address, token_id) DO UPDATE SET
                            decimals = EXCLUDED.decimals,
                            platform = EXCLUDED.platform,
                            total_supply = EXCLUDED.total_supply;
                        """

                        # Convert total_supply to int if it's a string, handle large numbers
                        total_supply = token.get("total_supply", 0)
                        if isinstance(total_supply, str):
                            try:
                                total_supply = int(total_supply)
                            except ValueError:
                                total_supply = 0

                        conn.execute(
                            text(platforms_sql),
                            {
                                "address": platform_address,
                                "token_id": token["symbol"],
                                "decimals": token.get("decimals", 0),
                                "platform": platform,
                                "total_supply": total_supply,
                            },
                        )

                conn.commit()
                logger.info("Successfully inserted all token data")

        except Exception as e:
            logger.error(f"Error inserting token data: {e}")
            raise

    def get_token_info_from_blockchain(
        self, addresses: List[str]
    ) -> Optional[List[Dict[str, Any]]]:
        """Get token information from contract"""
        try:
            with open(Path.home() / "dynamic_whitelist" / "data" / "BatchTokenInfo.json", "r") as f:
                batch_token_info = json.load(f)
                contract_bytecode = batch_token_info["bytecode"]["object"]

            tokens_encoded = encode(["address[]"], [addresses])
            input_data = contract_bytecode + tokens_encoded.hex()
            encoded_return_data = self.web3.eth.call({"data": input_data})

            # Each token info is 4 x 32 bytes = 128 bytes
            tokens_metadata = []
            for i in range(len(addresses)):
                try:
                    start_idx = i * 128
                    name_bytes = encoded_return_data[start_idx : start_idx + 32]
                    symbol_bytes = encoded_return_data[start_idx + 32 : start_idx + 64]
                    decimals_bytes = encoded_return_data[start_idx + 64 : start_idx + 96]
                    total_supply_bytes = encoded_return_data[start_idx + 96 : start_idx + 128]

                    token_info = {}
                    # Remove null bytes from strings and handle decode errors
                    try:
                        token_info["name"] = name_bytes.split(b"\x00")[0].decode("utf-8")
                    except (UnicodeDecodeError, IndexError):
                        token_info["name"] = "Unknown Token"
                    
                    try:
                        token_info["symbol"] = symbol_bytes.split(b"\x00")[0].decode("utf-8")
                    except (UnicodeDecodeError, IndexError):
                        token_info["symbol"] = "UNK"
                    
                    # Decimals is a uint8 in the last byte
                    token_info["decimals"] = int(decimals_bytes[-1]) if decimals_bytes else None
                    # Total supply is a uint256
                    token_info["total_supply"] = int.from_bytes(total_supply_bytes, "big") if total_supply_bytes else 0

                    tokens_metadata.append(token_info)
                except Exception as e:
                    logger.warning(f"Error processing token {addresses[i]}: {e}")
                    # Add default token info for failed tokens
                    tokens_metadata.append({
                        "name": "Unknown Token",
                        "symbol": "UNK",
                        "decimals": 0,
                        "total_supply": 0
                    })

            return tokens_metadata
        except Exception as e:
            logger.error(f"Error getting token info from blockchain: {e}")
            return None

    def save_results_to_file(self, filename: str = "missing_tokens_results.json"):
        """Save results to JSON file for backup/analysis"""
        results = {
            "total_ethereum_tokens": len(self.ethereum_tokens),
            "total_missing_tokens": len(self.missing_tokens),
            "successfully_scraped": len(self.new_token_data),
            "missing_tokens_list": list(self.missing_tokens),
            "scraped_token_data": self.new_token_data,
        }

        with open(filename, "w") as f:
            json.dump(results, f, indent=2)

        logger.info(f"Results saved to {filename}")

    def get_summary_statistics(self):
        """Get summary statistics of the comparison and scraping results"""
        stats = {
            "total_ethereum_tokens": len(self.ethereum_tokens),
            "total_missing_tokens": len(self.missing_tokens),
            "successfully_scraped": len(self.new_token_data),
            "scraping_success_rate": len(self.new_token_data)
            / len(self.missing_tokens)
            * 100
            if self.missing_tokens
            else 0,
        }

        logger.info("=== SUMMARY STATISTICS ===")
        logger.info(
            f"Total Ethereum tokens in file: {stats['total_ethereum_tokens']:,}"
        )
        logger.info(
            f"Missing tokens from CoinGecko DB: {stats['total_missing_tokens']:,}"
        )
        logger.info(f"Successfully scraped tokens: {stats['successfully_scraped']:,}")
        logger.info(f"Scraping success rate: {stats['scraping_success_rate']:.2f}%")

        return stats

    def process_missing_tokens(self, missing_tokens: Set[str]):
        """Process missing tokens"""
        

    def run_full_process(
        self,
        scrape_limit: Optional[int] = None,
        missing_tokens: Optional[Set[str]] = None,
    ):
        """Run the complete process"""
        logger.info("Starting token comparison and scraping process")

        # Step 1: Find missing tokens
        if missing_tokens is None:
            missing_tokens = self.find_missing_tokens()
        if not missing_tokens:
            logger.info("No missing tokens found. Process complete.")
            return

        # Step 2: Create database tables
        self.create_missing_tokens_table()

       
        
        tokens_to_process = list(missing_tokens)
        if scrape_limit:
            tokens_to_process = tokens_to_process[:scrape_limit]
        for i in range(0, len(tokens_to_process), 50):
            batch = tokens_to_process[i:i+50]
            tokens_metadata = self.get_token_info_from_blockchain(batch)
            if tokens_metadata:
                for j, metadata in enumerate(tokens_metadata):
                    address = batch[j].lower()
                    if not metadata['name'] or not metadata['symbol']:
                        continue
                    
                    token_data = {
                        "name": metadata["name"],
                        "symbol": metadata["symbol"].lower(),
                        "decimals": metadata["decimals"],
                        "total_supply": str(metadata["total_supply"]),
                        "platforms": {
                            "ethereum": address
                        },  # Keep platform data for insert method
                    }
                    self.new_token_data.append(token_data)
            time.sleep(3)
            logger.info(f"Processed {i}:{i+50} of {len(tokens_to_process)} tokens")

        

        # Step 5: Insert all collected data into database
        if self.new_token_data:
            self.insert_token_data()

        # Step 6: Save results and show summary
        self.save_results_to_file()
        self.get_summary_statistics()

        logger.info("Process completed successfully!")


def main():
    """Main function to run the token comparison and scraping"""
    try:
        # Initialize the comparator
        comparator = CoinGeckoTokenComparator()

        # Run the full process
        # For testing, limit to first 50 tokens: scrape_limit=50
        # For full run, remove the limit or set to None
        comparator.run_full_process(scrape_limit=50)  # Remove limit for full run

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    comparator = CoinGeckoTokenComparator()
    comparator.run_full_process()