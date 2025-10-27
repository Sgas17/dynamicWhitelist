"""
Token blacklist manager for filtering phishing/scam tokens.

This module maintains a blacklist of malicious tokens by combining:
1. Etherscan label scraping (phishing/scam detection)
2. Manual blacklist additions
"""

import json
import logging
from pathlib import Path
from typing import Set, Dict, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

from src.utils.etherscan_label_scraper import EtherscanLabelScraper, EtherscanLabel

logger = logging.getLogger(__name__)


@dataclass
class BlacklistEntry:
    """Blacklist entry with metadata."""

    address: str
    reason: str  # "phishing", "scam", "rug_pull", "manual", etc.
    source: str  # "etherscan", "manual"
    added_at: str  # ISO timestamp
    nametag: Optional[str] = None
    notes: Optional[str] = None


class TokenBlacklistManager:
    """
    Manage token blacklist from Etherscan labels and manual entries.

    Maintains a cached blacklist file for fast lookups during transfer processing.
    """

    def __init__(
        self,
        blacklist_file: str = "data/token_blacklist.json",
        cache_file: str = "data/etherscan_cache/etherscan_labels.json",
        auto_update: bool = False,
        update_interval_hours: int = 24,
    ):
        """
        Initialize blacklist manager.

        Args:
            blacklist_file: Path to blacklist JSON file
            cache_file: Path to Etherscan cache file
            auto_update: Auto-update from Etherscan on load
            update_interval_hours: Hours between auto-updates
        """
        self.blacklist_file = Path(blacklist_file)
        self.blacklist_file.parent.mkdir(parents=True, exist_ok=True)

        self.cache_file = Path(cache_file)
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)

        self.update_interval = timedelta(hours=update_interval_hours)

        # Load blacklist
        self.blacklist: Dict[str, BlacklistEntry] = self._load_blacklist()

        # Initialize Etherscan scraper
        self.scraper = EtherscanLabelScraper(
            cache_dir=str(self.cache_file.parent)
        )

        # Auto-update if needed
        if auto_update and self._should_update():
            logger.info("Auto-update is enabled but requires manual check_and_add_from_etherscan call")

    def _load_blacklist(self) -> Dict[str, BlacklistEntry]:
        """Load blacklist from disk."""
        if self.blacklist_file.exists():
            try:
                with open(self.blacklist_file, "r") as f:
                    data = json.load(f)

                blacklist = {}
                for addr, entry_data in data.get("blacklist", {}).items():
                    blacklist[addr.lower()] = BlacklistEntry(**entry_data)

                logger.info(
                    f"Loaded {len(blacklist)} blacklisted tokens from {self.blacklist_file}"
                )
                return blacklist

            except Exception as e:
                logger.error(f"Failed to load blacklist: {e}")

        return {}

    def _save_blacklist(self):
        """Save blacklist to disk."""
        try:
            data = {
                "last_updated": datetime.now().isoformat(),
                "blacklist": {
                    addr: asdict(entry) for addr, entry in self.blacklist.items()
                },
            }

            with open(self.blacklist_file, "w") as f:
                json.dump(data, f, indent=2)

            logger.info(
                f"Saved {len(self.blacklist)} blacklisted tokens to {self.blacklist_file}"
            )

        except Exception as e:
            logger.error(f"Failed to save blacklist: {e}")

    def _should_update(self) -> bool:
        """Check if blacklist should be updated based on last update time."""
        if not self.blacklist_file.exists():
            return True

        try:
            with open(self.blacklist_file, "r") as f:
                data = json.load(f)
                last_updated_str = data.get("last_updated")

                if last_updated_str:
                    last_updated = datetime.fromisoformat(last_updated_str)
                    return datetime.now() - last_updated > self.update_interval

        except Exception as e:
            logger.warning(f"Could not check last update time: {e}")

        return True

    def check_and_add_from_etherscan(self, addresses: List[str]) -> int:
        """
        Check addresses on Etherscan and add phishing/scam tokens to blacklist.

        Args:
            addresses: List of addresses to check

        Returns:
            Number of new blacklisted addresses added
        """
        added_count = 0

        for address in addresses:
            address = address.lower()

            # Skip if already blacklisted
            if address in self.blacklist:
                continue

            # Check Etherscan
            label = self.scraper.get_address_label(address)

            if label.is_phishing or label.is_scam:
                reason = "phishing" if label.is_phishing else "scam"

                self.blacklist[address] = BlacklistEntry(
                    address=address,
                    reason=reason,
                    source="etherscan",
                    added_at=datetime.now().isoformat(),
                    nametag=label.nametag,
                    notes=label.warning,
                )
                added_count += 1

                logger.warning(
                    f"Blacklisted {address}: {label.nametag} ({reason})"
                )

        if added_count > 0:
            self._save_blacklist()

        return added_count

    def add_manual_entry(
        self, address: str, reason: str, notes: Optional[str] = None
    ):
        """
        Manually add token to blacklist.

        Args:
            address: Token address
            reason: Reason for blacklisting
            notes: Optional notes
        """
        address = address.lower()

        self.blacklist[address] = BlacklistEntry(
            address=address,
            reason=reason,
            source="manual",
            added_at=datetime.now().isoformat(),
            notes=notes,
        )

        self._save_blacklist()
        logger.info(f"Manually blacklisted {address}: {reason}")

    def remove_entry(self, address: str):
        """Remove token from blacklist."""
        address = address.lower()

        if address in self.blacklist:
            del self.blacklist[address]
            self._save_blacklist()
            logger.info(f"Removed {address} from blacklist")

    def is_blacklisted(self, address: str) -> bool:
        """Check if address is blacklisted."""
        return address.lower() in self.blacklist

    def get_entry(self, address: str) -> Optional[BlacklistEntry]:
        """Get blacklist entry for address."""
        return self.blacklist.get(address.lower())

    def filter_addresses(self, addresses: List[str]) -> List[str]:
        """
        Filter out blacklisted addresses.

        Args:
            addresses: List of addresses to filter

        Returns:
            List of addresses that are NOT blacklisted
        """
        clean = []
        filtered = []

        for address in addresses:
            if self.is_blacklisted(address):
                entry = self.get_entry(address)
                filtered.append(address)
                logger.debug(
                    f"Filtered blacklisted token {address}: "
                    f"{entry.nametag or entry.reason} ({entry.source})"
                )
            else:
                clean.append(address)

        if filtered:
            logger.info(
                f"Filtered {len(filtered)}/{len(addresses)} blacklisted tokens"
            )

        return clean

    def get_stats(self) -> Dict[str, int]:
        """Get blacklist statistics."""
        stats = {
            "total": len(self.blacklist),
            "etherscan": 0,
            "manual": 0,
            "phishing": 0,
            "scam": 0,
            "other": 0,
        }

        for entry in self.blacklist.values():
            # Count by source
            if entry.source in stats:
                stats[entry.source] += 1

            # Count by reason
            if entry.reason == "phishing":
                stats["phishing"] += 1
            elif entry.reason == "scam":
                stats["scam"] += 1
            else:
                stats["other"] += 1

        return stats

    def export_addresses(self, output_file: Optional[str] = None) -> List[str]:
        """
        Export list of blacklisted addresses.

        Args:
            output_file: Optional file to write addresses to

        Returns:
            List of blacklisted addresses
        """
        addresses = list(self.blacklist.keys())

        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w") as f:
                for address in addresses:
                    f.write(f"{address}\n")

            logger.info(f"Exported {len(addresses)} addresses to {output_file}")

        return addresses


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Initialize manager
    manager = TokenBlacklistManager(auto_update=False)

    # Check some known tokens
    test_addresses = [
        "0x20a41ff9a18ea1b050253d1db297dc73b9b5f258",  # Known phishing
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT (safe)
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC (safe)
    ]

    # Check and add from Etherscan
    added = manager.check_and_add_from_etherscan(test_addresses)
    print(f"\nAdded {added} new blacklisted tokens from Etherscan")

    # Filter addresses
    clean_addresses = manager.filter_addresses(test_addresses)
    print(f"\nClean addresses: {len(clean_addresses)}/{len(test_addresses)}")

    # Show stats
    stats = manager.get_stats()
    print("\nBlacklist Statistics:")
    print("=" * 50)
    for key, value in stats.items():
        print(f"{key:15s}: {value:5d}")

    # Show some blacklisted entries
    print("\nSample Blacklisted Tokens:")
    print("=" * 50)
    for i, (addr, entry) in enumerate(list(manager.blacklist.items())[:5]):
        print(f"\n{i+1}. {addr}")
        print(f"   Nametag: {entry.nametag}")
        print(f"   Reason: {entry.reason}")
        print(f"   Source: {entry.source}")
