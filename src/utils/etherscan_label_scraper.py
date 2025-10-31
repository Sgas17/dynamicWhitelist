"""
Etherscan label scraper to identify phishing/scam tokens without API Pro plan.

This module scrapes token/address labels directly from Etherscan's public pages
to identify phishing tokens, scams, and other malicious addresses.
"""

import time
import logging
from typing import Optional, Dict, List
from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import json

logger = logging.getLogger(__name__)


@dataclass
class EtherscanLabel:
    """Etherscan address label information."""

    address: str
    nametag: Optional[str] = None
    label: Optional[str] = None
    warning: Optional[str] = None
    is_phishing: bool = False
    is_scam: bool = False
    reported_by: Optional[str] = None


class EtherscanLabelScraper:
    """Scrape token/address labels from Etherscan without API Pro plan."""

    BASE_URL = "https://etherscan.io"
    RATE_LIMIT_DELAY = 0.5  # seconds between requests to avoid rate limiting

    def __init__(self, cache_dir: Optional[str] = None):
        """
        Initialize scraper.

        Args:
            cache_dir: Directory to cache scraped labels (optional)
        """
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
            }
        )

        if cache_dir:
            self.cache_dir = Path(cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.cache_file = self.cache_dir / "etherscan_labels.json"
            self.cache = self._load_cache()
        else:
            self.cache_dir = None
            self.cache = {}

    def _load_cache(self) -> Dict[str, EtherscanLabel]:
        """Load cached labels from disk."""
        if self.cache_file and self.cache_file.exists():
            try:
                with open(self.cache_file, "r") as f:
                    data = json.load(f)
                    return {
                        addr: EtherscanLabel(**label_data)
                        for addr, label_data in data.items()
                    }
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
        return {}

    def _save_cache(self):
        """Save labels cache to disk."""
        if self.cache_dir and self.cache_file:
            try:
                data = {
                    addr: {
                        "address": label.address,
                        "nametag": label.nametag,
                        "label": label.label,
                        "warning": label.warning,
                        "is_phishing": label.is_phishing,
                        "is_scam": label.is_scam,
                        "reported_by": label.reported_by,
                    }
                    for addr, label in self.cache.items()
                }
                with open(self.cache_file, "w") as f:
                    json.dump(data, f, indent=2)
            except Exception as e:
                logger.warning(f"Failed to save cache: {e}")

    def get_address_label(
        self, address: str, use_cache: bool = True
    ) -> EtherscanLabel:
        """
        Get label information for an address.

        Args:
            address: Ethereum address (with or without 0x prefix)
            use_cache: Use cached result if available

        Returns:
            EtherscanLabel with scraped information
        """
        # Normalize address
        if not address.startswith("0x"):
            address = f"0x{address}"
        address = address.lower()

        # Check cache
        if use_cache and address in self.cache:
            logger.debug(f"Using cached label for {address}")
            return self.cache[address]

        # Scrape from Etherscan
        label = self._scrape_address_page(address)

        # Cache result
        self.cache[address] = label
        if self.cache_dir:
            self._save_cache()

        # Rate limiting
        time.sleep(self.RATE_LIMIT_DELAY)

        return label

    def _scrape_address_page(self, address: str) -> EtherscanLabel:
        """Scrape label info from Etherscan address page."""
        url = f"{self.BASE_URL}/address/{address}"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Initialize label
            label = EtherscanLabel(address=address)

            # Try to find nametag - look for specific ID or class patterns
            # Method 1: Look for nametag in page title or header
            title_elem = soup.find("title")
            if title_elem:
                title_text = title_elem.text
                # Nametag is usually in format "Nametag | Address 0x... | Etherscan"
                if "|" in title_text:
                    parts = title_text.split("|")
                    if len(parts) >= 2:
                        potential_nametag = parts[0].strip()
                        # Filter out generic text
                        if potential_nametag and potential_nametag not in [
                            "Address",
                            "Contract",
                            "Token",
                        ]:
                            label.nametag = potential_nametag

            # Method 2: Look for specific nametag span
            if not label.nametag:
                # Look for spans that contain "Fake_" or other nametag patterns
                all_spans = soup.find_all("span")
                for span in all_spans:
                    text = span.text.strip()
                    # Nametags often start with specific patterns
                    if any(
                        text.startswith(prefix)
                        for prefix in ["Fake_", "Phish", "Scam", "MEV"]
                    ) and len(text) > 3:
                        label.nametag = text
                        break

            # Try to find label badge (e.g., "Phish / Hack")
            # Labels are often in badge elements or specific divs
            badge_elems = soup.find_all("span", class_=lambda x: x and "badge" in x)
            for badge in badge_elems:
                badge_text = badge.text.strip()
                if any(
                    keyword in badge_text.lower()
                    for keyword in ["phish", "hack", "scam", "fake"]
                ):
                    label.label = badge_text
                    break

            # Also check for label in divs near the nametag
            if not label.label:
                label_divs = soup.find_all(
                    "div", class_=lambda x: x and "label" in str(x).lower()
                )
                for div in label_divs:
                    text = div.text.strip()
                    if "/" in text and any(
                        keyword in text.lower()
                        for keyword in ["phish", "hack", "scam"]
                    ):
                        label.label = text
                        break

            # Try to find warning message
            # Warnings are typically in alert boxes
            alert_elem = soup.find(
                "div",
                class_=lambda x: x and any(c in str(x) for c in ["alert", "warning"]),
            )
            if alert_elem:
                warning_text = alert_elem.text.strip()
                if "phishing" in warning_text.lower() or "scam" in warning_text.lower():
                    label.warning = warning_text

                    # Try to extract reporter
                    if "reported by" in warning_text.lower():
                        # Extract reporter name after "Reported by"
                        parts = warning_text.lower().split("reported by")
                        if len(parts) > 1:
                            reporter = parts[1].strip().rstrip(".").split()[0]
                            label.reported_by = reporter

            # Determine if phishing/scam based on collected info
            phishing_keywords = ["phish", "fake_phishing"]
            scam_keywords = ["scam", "hack", "fake"]

            if label.nametag:
                nametag_lower = label.nametag.lower()
                label.is_phishing = any(
                    keyword in nametag_lower for keyword in phishing_keywords
                )
                label.is_scam = any(
                    keyword in nametag_lower for keyword in scam_keywords
                )

            if label.label:
                label_lower = label.label.lower()
                label.is_phishing = label.is_phishing or any(
                    keyword in label_lower for keyword in phishing_keywords
                )
                label.is_scam = label.is_scam or any(
                    keyword in label_lower for keyword in scam_keywords
                )

            if label.warning:
                warning_lower = label.warning.lower()
                label.is_phishing = label.is_phishing or "phishing" in warning_lower
                label.is_scam = label.is_scam or "scam" in warning_lower

            logger.info(
                f"Scraped {address}: nametag={label.nametag}, "
                f"phishing={label.is_phishing}, scam={label.is_scam}"
            )

            return label

        except requests.RequestException as e:
            logger.error(f"Failed to scrape {address}: {e}")
            return EtherscanLabel(address=address)

    def check_tokens_batch(
        self, addresses: List[str], use_cache: bool = True
    ) -> Dict[str, EtherscanLabel]:
        """
        Check labels for multiple addresses.

        Args:
            addresses: List of Ethereum addresses
            use_cache: Use cached results if available

        Returns:
            Dict mapping address to EtherscanLabel
        """
        results = {}

        for i, address in enumerate(addresses):
            logger.info(f"Checking address {i+1}/{len(addresses)}: {address}")
            results[address] = self.get_address_label(address, use_cache=use_cache)

        return results

    def get_phishing_addresses(self) -> List[str]:
        """Get list of all cached phishing addresses."""
        return [
            addr for addr, label in self.cache.items() if label.is_phishing or label.is_scam
        ]


# Convenience functions
def is_phishing_token(address: str, scraper: Optional[EtherscanLabelScraper] = None) -> bool:
    """
    Check if a token address is marked as phishing on Etherscan.

    Args:
        address: Token address to check
        scraper: Optional scraper instance (creates new one if not provided)

    Returns:
        True if address is marked as phishing/scam
    """
    if scraper is None:
        scraper = EtherscanLabelScraper()

    label = scraper.get_address_label(address)
    return label.is_phishing or label.is_scam


def filter_phishing_tokens(
    addresses: List[str], scraper: Optional[EtherscanLabelScraper] = None
) -> List[str]:
    """
    Filter out phishing tokens from a list of addresses.

    Args:
        addresses: List of token addresses
        scraper: Optional scraper instance

    Returns:
        List of addresses that are NOT marked as phishing
    """
    if scraper is None:
        scraper = EtherscanLabelScraper()

    clean_addresses = []
    for address in addresses:
        label = scraper.get_address_label(address)
        if not (label.is_phishing or label.is_scam):
            clean_addresses.append(address)
        else:
            logger.info(
                f"Filtered out {address}: {label.nametag} ({label.label})"
            )

    return clean_addresses


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Test with known phishing address
    scraper = EtherscanLabelScraper(cache_dir="data/etherscan_cache")

    test_addresses = [
        "0x20a41ff9a18ea1b050253d1db297dc73b9b5f258",  # Known phishing
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT (safe)
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC (safe)
    ]

    results = scraper.check_tokens_batch(test_addresses)

    print("\nResults:")
    print("=" * 80)
    for address, label in results.items():
        print(f"\nAddress: {address}")
        print(f"  Nametag: {label.nametag}")
        print(f"  Label: {label.label}")
        print(f"  Is Phishing: {label.is_phishing}")
        print(f"  Is Scam: {label.is_scam}")
        if label.warning:
            print(f"  Warning: {label.warning[:100]}...")
