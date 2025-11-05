"""
PostgreSQL storage implementation for the dynamic whitelist system.
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg
from asyncpg.pool import Pool

from .base import (
    ConnectionError,
    DataError,
    PoolStorageInterface,
    StorageBase,
    TokenStorageInterface,
    TransactionManager,
)

logger = logging.getLogger(__name__)


def normalize_address(address: Any) -> Optional[str]:
    """
    Normalize Ethereum address to lowercase for consistent storage.

    Args:
        address: Address as string, bytes, or None

    Returns:
        Lowercase address string or None
    """
    if address is None:
        return None
    if isinstance(address, bytes):
        return address.hex().lower()
    return str(address).lower()


class PostgresStorage(
    StorageBase, TokenStorageInterface, PoolStorageInterface, TransactionManager
):
    """
    PostgreSQL storage implementation supporting async operations.

    Works with existing database schema:
    - network_{CHAIN_ID}_dex_pools_cryo for pool data
    - Token metadata tables for coin information
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL storage.

        Args:
            config: Configuration with keys:
                - host: PostgreSQL host
                - port: PostgreSQL port
                - user: Database user
                - password: Database password
                - database: Database name
                - pool_size: Connection pool size (default: 10)
                - pool_timeout: Pool timeout in seconds (default: 10)
        """
        super().__init__(config)
        self.pool: Optional[Pool] = None
        self.pool_size = config.get("pool_size", 10)
        self.pool_timeout = config.get("pool_timeout", 10)

    async def connect(self) -> None:
        """Establish connection pool to PostgreSQL."""
        try:
            dsn = (
                f"postgresql://{self.config['user']}:{self.config['password']}@"
                f"{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )

            self.pool = await asyncpg.create_pool(
                dsn,
                min_size=2,
                max_size=self.pool_size,
                timeout=self.pool_timeout,
                command_timeout=60,
            )
            self.is_connected = True
            logger.info("PostgreSQL connection pool established")

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise ConnectionError(f"PostgreSQL connection failed: {e}")

    async def disconnect(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self.is_connected = False
            logger.info("PostgreSQL connection pool closed")

    async def health_check(self) -> bool:
        """Check PostgreSQL connection health."""
        if not self.pool:
            return False

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False

    # Token Storage Implementation (using existing token metadata table)

    async def store_token(self, token: Dict[str, Any], chain: str) -> bool:
        """
        Store a single token in the CoinGecko tokens table.

        Args:
            token: Token data containing CoinGecko ID, symbol, name, etc.
            chain: Chain identifier (used for platform data)

        Returns:
            bool: True if stored successfully
        """
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Store in main coingecko_tokens table
                    token_query = """
                        INSERT INTO coingecko_tokens (id, symbol, name, market_cap_rank)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (id) 
                        DO UPDATE SET 
                            symbol = EXCLUDED.symbol,
                            name = EXCLUDED.name,
                            market_cap_rank = EXCLUDED.market_cap_rank
                    """

                    await conn.execute(
                        token_query,
                        token.get("id", token.get("coingecko_id", "")),
                        token.get("symbol"),
                        token.get("name"),
                        token.get("market_cap_rank", 0),
                    )

                    # If we have address/platform data, store in coingecko_token_platforms
                    if token.get("address") and token.get("platform"):
                        platform_query = """
                            INSERT INTO coingecko_token_platforms (address, token_id, decimals, platform, total_supply)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (address, platform)
                            DO UPDATE SET
                                token_id = EXCLUDED.token_id,
                                decimals = EXCLUDED.decimals,
                                total_supply = EXCLUDED.total_supply
                        """

                        await conn.execute(
                            platform_query,
                            token["address"].lower(),
                            token.get("id", token.get("coingecko_id", "")),
                            token.get("decimals"),
                            token.get("platform", chain),
                            token.get("total_supply"),
                        )

                    # If this is missing data for Ethereum, also store in missing_coingecko_tokens_ethereum
                    elif (
                        chain == "ethereum"
                        and token.get("address")
                        and not token.get("id")
                    ):
                        missing_query = """
                            INSERT INTO missing_coingecko_tokens_ethereum (address, symbol, decimals, name)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (address)
                            DO UPDATE SET
                                symbol = EXCLUDED.symbol,
                                decimals = EXCLUDED.decimals,
                                name = EXCLUDED.name
                        """

                        await conn.execute(
                            missing_query,
                            token["address"].lower(),
                            token.get("symbol"),
                            token.get("decimals"),
                            token.get("name"),
                        )

                    return True

        except Exception as e:
            logger.error(f"Failed to store token {token.get('symbol')}: {e}")
            raise DataError(f"Token storage failed: {e}")

    async def store_tokens_batch(self, tokens: List[Dict[str, Any]], chain: str) -> int:
        """
        Store multiple tokens in batch using CoinGecko schema.

        Args:
            tokens: List of token data
            chain: Chain identifier

        Returns:
            int: Number of tokens stored
        """
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        if not tokens:
            return 0

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    stored_count = 0

                    # Separate tokens with CoinGecko ID from those without
                    coingecko_tokens = [
                        t for t in tokens if t.get("id") or t.get("coingecko_id")
                    ]
                    missing_tokens = [
                        t
                        for t in tokens
                        if not (t.get("id") or t.get("coingecko_id"))
                        and t.get("address")
                    ]

                    # Batch insert CoinGecko tokens
                    if coingecko_tokens:
                        token_records = [
                            (
                                token.get("id", token.get("coingecko_id", "")),
                                token.get("symbol"),
                                token.get("name"),
                                token.get("market_cap_rank", 0),
                            )
                            for token in coingecko_tokens
                        ]

                        token_query = """
                            INSERT INTO coingecko_tokens (id, symbol, name, market_cap_rank)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (id)
                            DO UPDATE SET
                                symbol = EXCLUDED.symbol,
                                name = EXCLUDED.name,
                                market_cap_rank = EXCLUDED.market_cap_rank
                        """

                        await conn.executemany(token_query, token_records)

                        # Insert platform data for tokens with addresses
                        platform_records = [
                            (
                                token["address"].lower(),
                                token.get("id", token.get("coingecko_id", "")),
                                token.get("decimals"),
                                token.get("platform", chain),
                                token.get("total_supply"),
                            )
                            for token in coingecko_tokens
                            if token.get("address")
                        ]

                        if platform_records:
                            platform_query = """
                                INSERT INTO coingecko_token_platforms (address, token_id, decimals, platform, total_supply)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (address, platform)
                                DO UPDATE SET
                                    token_id = EXCLUDED.token_id,
                                    decimals = EXCLUDED.decimals,
                                    total_supply = EXCLUDED.total_supply
                            """

                            await conn.executemany(platform_query, platform_records)

                        stored_count += len(coingecko_tokens)

                    # Handle missing tokens for Ethereum
                    if missing_tokens and chain == "ethereum":
                        missing_records = [
                            (
                                token["address"].lower(),
                                token.get("symbol"),
                                token.get("decimals"),
                                token.get("name"),
                            )
                            for token in missing_tokens
                        ]

                        missing_query = """
                            INSERT INTO missing_coingecko_tokens_ethereum (address, symbol, decimals, name)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (address)
                            DO UPDATE SET
                                symbol = EXCLUDED.symbol,
                                decimals = EXCLUDED.decimals,
                                name = EXCLUDED.name
                        """

                        await conn.executemany(missing_query, missing_records)
                        stored_count += len(missing_tokens)

                    # Handle missing platform data for other chains
                    elif missing_tokens:
                        missing_platform_records = [
                            (
                                token["address"].lower(),
                                token.get(
                                    "token_id", ""
                                ),  # May be empty for missing data
                                token.get("decimals"),
                                chain,
                                token.get("total_supply"),
                            )
                            for token in missing_tokens
                        ]

                        missing_platform_query = """
                            INSERT INTO missing_coingecko_tokens_platforms (address, token_id, decimals, platform, total_supply)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (address, platform)
                            DO UPDATE SET
                                token_id = EXCLUDED.token_id,
                                decimals = EXCLUDED.decimals,
                                total_supply = EXCLUDED.total_supply
                        """

                        await conn.executemany(
                            missing_platform_query, missing_platform_records
                        )
                        stored_count += len(missing_tokens)

                    logger.info(f"Stored {stored_count} tokens for {chain}")
                    return stored_count

        except Exception as e:
            logger.error(f"Failed to batch store tokens: {e}")
            raise DataError(f"Batch token storage failed: {e}")

    async def get_token(self, address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single token by address, checking both CoinGecko tables and missing tables."""
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        try:
            async with self.pool.acquire() as conn:
                # First try coingecko_token_platforms joined with coingecko_tokens
                query = """
                    SELECT 
                        ct.id, ct.symbol, ct.name, ct.market_cap_rank,
                        ctp.address, ctp.decimals, ctp.platform, ctp.total_supply
                    FROM coingecko_tokens ct
                    JOIN coingecko_token_platforms ctp ON ct.id = ctp.token_id
                    WHERE ctp.address = $1 AND ctp.platform = $2
                """

                row = await conn.fetchrow(query, address.lower(), chain)
                if row:
                    return dict(row)

                # If not found and chain is ethereum, try missing_coingecko_tokens_ethereum
                if chain == "ethereum":
                    missing_query = """
                        SELECT address, symbol, decimals, name
                        FROM missing_coingecko_tokens_ethereum
                        WHERE address = $1
                    """

                    row = await conn.fetchrow(missing_query, address.lower())
                    if row:
                        return dict(row)

                return None

        except Exception as e:
            logger.error(f"Failed to get token {address}: {e}")
            raise DataError(f"Token retrieval failed: {e}")

    async def get_whitelisted_tokens(self, chain: str) -> List[Dict[str, Any]]:
        """Get all tokens for a specific chain from CoinGecko tables and missing tables."""
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        try:
            async with self.pool.acquire() as conn:
                tokens = []

                # Get tokens from coingecko_token_platforms joined with coingecko_tokens
                main_query = """
                    SELECT 
                        ct.id, ct.symbol, ct.name, ct.market_cap_rank,
                        ctp.address, ctp.decimals, ctp.platform, ctp.total_supply
                    FROM coingecko_tokens ct
                    JOIN coingecko_token_platforms ctp ON ct.id = ctp.token_id
                    WHERE ctp.platform = $1
                    ORDER BY ct.market_cap_rank NULLS LAST, ct.symbol
                """

                rows = await conn.fetch(main_query, chain)
                tokens.extend([dict(row) for row in rows])

                # If ethereum, also get from missing_coingecko_tokens_ethereum
                if chain == "ethereum":
                    missing_query = """
                        SELECT address, symbol, decimals, name, 
                               NULL as id, NULL as market_cap_rank, 
                               'ethereum' as platform, NULL as total_supply
                        FROM missing_coingecko_tokens_ethereum
                        ORDER BY symbol
                    """

                    missing_rows = await conn.fetch(missing_query)
                    tokens.extend([dict(row) for row in missing_rows])

                return tokens

        except Exception as e:
            logger.error(f"Failed to get tokens for {chain}: {e}")
            raise DataError(f"Token retrieval failed: {e}")

    async def update_token_status(self, address: str, chain: str, status: str) -> bool:
        """Update token (no whitelist status in current schema)."""
        # Current schema doesn't have whitelist status, so this is a no-op
        logger.warning("Token whitelist status not supported in current schema")
        return True

    # Pool Storage Implementation (using existing network_{CHAIN_ID}_dex_pools_cryo)

    def _get_pool_table_name(self, chain: str) -> str:
        """Get the pool table name for a specific chain."""
        chain_id_map = {"ethereum": "1", "base": "8453", "arbitrum": "42161"}
        chain_id = chain_id_map.get(chain, "1")
        return f"network_{chain_id}_dex_pools_cryo"

    async def store_pool(self, pool: Dict[str, Any], chain: str, protocol: str) -> bool:
        """Store a single pool using existing schema."""
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        table_name = self._get_pool_table_name(chain)

        query = f"""
            INSERT INTO {table_name} (
                address, factory, asset0, asset1, asset2, asset3,
                creation_block, fee, additional_data, priority, tick_spacing
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (address)
            DO UPDATE SET
                factory = EXCLUDED.factory,
                asset0 = EXCLUDED.asset0,
                asset1 = EXCLUDED.asset1,
                creation_block = EXCLUDED.creation_block,
                fee = EXCLUDED.fee,
                additional_data = EXCLUDED.additional_data,
                tick_spacing = EXCLUDED.tick_spacing
            RETURNING address
        """

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval(
                    query,
                    pool["address"],
                    pool.get("factory"),
                    pool.get("asset0"),
                    pool.get("asset1"),
                    None,  # asset2
                    None,  # asset3
                    pool.get("creation_block"),
                    pool.get("fee"),
                    json.dumps(pool.get("additional_data", {})),
                    None,  # priority
                    pool.get("tick_spacing"),
                )
                return result is not None

        except Exception as e:
            logger.error(f"Failed to store pool {pool.get('address')}: {e}")
            raise DataError(f"Pool storage failed: {e}")

    async def store_pools_batch(
        self, pools: List[Dict[str, Any]], chain: str, protocol: str
    ) -> int:
        """Store multiple pools in batch with normalized lowercase addresses."""
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        if not pools:
            return 0

        table_name = self._get_pool_table_name(chain)

        # Normalize all addresses to lowercase
        records = [
            (
                normalize_address(pool["address"]),
                normalize_address(pool.get("factory")),
                normalize_address(pool.get("asset0")),
                normalize_address(pool.get("asset1")),
                None,  # asset2
                None,  # asset3
                pool.get("creation_block"),
                pool.get("fee"),
                json.dumps(pool.get("additional_data", {})),
                None,  # priority
                pool.get("tick_spacing"),
            )
            for pool in pools
        ]

        query = f"""
            INSERT INTO {table_name} (
                address, factory, asset0, asset1, asset2, asset3,
                creation_block, fee, additional_data, priority, tick_spacing
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (address)
            DO UPDATE SET
                factory = EXCLUDED.factory,
                asset0 = EXCLUDED.asset0,
                asset1 = EXCLUDED.asset1,
                creation_block = EXCLUDED.creation_block,
                fee = EXCLUDED.fee,
                additional_data = EXCLUDED.additional_data,
                tick_spacing = EXCLUDED.tick_spacing
        """

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(query, records)
                    count = len(records)
                    logger.info(f"Stored {count} pools for {chain}/{protocol}")
                    return count

        except Exception as e:
            logger.error(f"Failed to batch store pools: {e}")
            raise DataError(f"Batch pool storage failed: {e}")

    async def get_pool(self, address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single pool by address."""
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        table_name = self._get_pool_table_name(chain)

        query = f"""
            SELECT address, factory, asset0, asset1, creation_block,
                   fee, additional_data, tick_spacing
            FROM {table_name}
            WHERE address = $1
        """

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, address.lower())
                if row:
                    result = dict(row)
                    # Parse JSON additional_data
                    if result.get("additional_data"):
                        try:
                            result["additional_data"] = json.loads(
                                result["additional_data"]
                            )
                        except:
                            pass
                    return result
                return None

        except Exception as e:
            logger.error(f"Failed to get pool {address}: {e}")
            raise DataError(f"Pool retrieval failed: {e}")

    async def get_active_pools(
        self, chain: str, protocol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all pools for a chain."""
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        table_name = self._get_pool_table_name(chain)

        query = f"""
            SELECT address, factory, asset0, asset1, creation_block,
                   fee, additional_data, tick_spacing
            FROM {table_name}
            ORDER BY creation_block DESC
        """

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
                results = []
                for row in rows:
                    result = dict(row)
                    # Parse JSON additional_data
                    if result.get("additional_data"):
                        try:
                            result["additional_data"] = json.loads(
                                result["additional_data"]
                            )
                        except:
                            pass
                    results.append(result)
                return results

        except Exception as e:
            logger.error(f"Failed to get active pools: {e}")
            raise DataError(f"Active pools retrieval failed: {e}")

    async def update_pool_liquidity(
        self, address: str, chain: str, liquidity: float
    ) -> bool:
        """Update pool (liquidity not in current schema)."""
        # Current schema doesn't have liquidity field
        logger.warning("Pool liquidity not supported in current schema")
        return True

    # Transaction Management

    async def begin_transaction(self) -> Any:
        """Begin a new transaction."""
        if not self.pool:
            raise ConnectionError("Not connected to PostgreSQL")

        conn = await self.pool.acquire()
        transaction = conn.transaction()
        await transaction.start()
        return (conn, transaction)

    async def commit_transaction(self, transaction: Any) -> bool:
        """Commit a transaction."""
        conn, tx = transaction
        try:
            await tx.commit()
            return True
        finally:
            await self.pool.release(conn)

    async def rollback_transaction(self, transaction: Any) -> bool:
        """Rollback a transaction."""
        conn, tx = transaction
        try:
            await tx.rollback()
            return True
        finally:
            await self.pool.release(conn)
