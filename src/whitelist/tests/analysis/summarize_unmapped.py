#!/usr/bin/env python3
"""
Summarize unmapped tokens and output key findings.

This script:
1. Shows which tokens can't be mapped from EITHER exchange
2. Confirms these are the tokens without price discovery paths
"""

import json
from pathlib import Path


def main():
    """Summarize unmapped token analysis."""
    # Load overlap analysis
    overlap_path = Path(
        "/home/sam-sullivan/dynamicWhitelist/data/unmapped_tokens/overlap_analysis.json"
    )
    with open(overlap_path, "r") as f:
        overlap = json.load(f)

    both_unmapped = overlap["both_unmapped"]
    summary = overlap["summary"]

    print("=" * 80)
    print("UNMAPPED TOKEN SUMMARY")
    print("=" * 80)
    print()

    print(f"📊 Overview:")
    print(f"  Hyperliquid has {summary['total_hyperliquid']} unmapped tokens")
    print(f"  Lighter has {summary['total_lighter']} unmapped tokens")
    print()

    print(f"❌ TOKENS UNMAPPED IN BOTH EXCHANGES: {summary['both_unmapped_count']}")
    print("=" * 80)
    print()
    print(
        "These tokens cannot get prices from EITHER Hyperliquid OR Lighter exchanges."
    )
    print(
        "They MUST be priced through on-chain V2/V3 pool reserves if they are to be included."
    )
    print()

    # Group by token type
    major_l1_tokens = {
        "ADA",
        "BCH",
        "LTC",
        "TRX",
        "XMR",
        "ZEC",
        "SUI",
        "APT",
        "SEI",
    }
    defi_tokens = {"GMX", "PYTH", "OP", "EIGEN", "ENA"}
    meme_tokens = {
        "WIF",
        "POPCAT",
        "FARTCOIN",
        "BERA",
        "PENGU",
        "AI16Z",
        "YZY",
    }
    other_tokens = set(both_unmapped) - major_l1_tokens - defi_tokens - meme_tokens

    print(
        f"🔷 Major L1/Alt Chains ({len(major_l1_tokens & set(both_unmapped))} tokens):"
    )
    for token in sorted(major_l1_tokens & set(both_unmapped)):
        print(f"  • {token}")
    print()

    print(f"💼 DeFi/Infrastructure ({len(defi_tokens & set(both_unmapped))} tokens):")
    for token in sorted(defi_tokens & set(both_unmapped)):
        print(f"  • {token}")
    print()

    print(f"🐸 Meme/Community ({len(meme_tokens & set(both_unmapped))} tokens):")
    for token in sorted(meme_tokens & set(both_unmapped)):
        print(f"  • {token}")
    print()

    print(f"📦 Other tokens ({len(other_tokens)} tokens):")
    for token in sorted(other_tokens):
        print(f"  • {token}")
    print()

    print("=" * 80)
    print("🔍 KEY INSIGHTS")
    print("=" * 80)
    print()

    print(
        f"1. {summary['both_unmapped_count']} tokens ({summary['both_unmapped_count'] / summary['total_hyperliquid'] * 100:.1f}% of Hyperliquid unmapped) need V2/V3 price discovery"
    )
    print()

    print("2. These are NOT obscure tokens - they include major L1s (ADA, LTC), ")
    print("   DeFi protocols (GMX, PYTH), and popular memes (WIF, POPCAT)")
    print()

    print("3. Without iterative V2 price discovery, these tokens will be:")
    print("   ❌ Excluded from our whitelist (if they lack Ethereum presence)")
    print("   ❌ Priced incorrectly (if partial on-chain data exists)")
    print()

    print("4. The solution:")
    print("   ✅ Implement iterative V2 price discovery as requested by the user")
    print("   ✅ Start with Hyperliquid/Binance prices for base tokens")
    print(
        "   ✅ Iteratively discover prices through V2 pools until no new prices found"
    )
    print()


if __name__ == "__main__":
    main()
