#!/usr/bin/env python3
"""
Analyze unmapped tokens from both Hyperliquid and Lighter exchanges.
Identifies tokens that can't be mapped from either source.
"""

import json
from pathlib import Path


def load_unmapped_tokens(filepath: str) -> dict:
    """Load unmapped tokens from JSON file."""
    with open(filepath, "r") as f:
        return json.load(f)


def analyze_unmapped_tokens():
    """Analyze and compare unmapped tokens from both exchanges."""
    # Load data
    hyperliquid = load_unmapped_tokens(
        "/home/sam-sullivan/dynamicWhitelist/data/unmapped_tokens/hyperliquid_unmapped.json"
    )
    lighter = load_unmapped_tokens(
        "/home/sam-sullivan/dynamicWhitelist/data/unmapped_tokens/lighter_unmapped.json"
    )

    # Convert to sets for comparison
    hyperliquid_set = set(hyperliquid["unmapped_symbols"])
    lighter_set = set(lighter["unmapped_symbols"])

    # Find overlaps
    both_unmapped = hyperliquid_set & lighter_set
    only_hyperliquid = hyperliquid_set - lighter_set
    only_lighter = lighter_set - hyperliquid_set

    # Print results
    print("=" * 80)
    print("UNMAPPED TOKEN ANALYSIS")
    print("=" * 80)
    print()

    print(f"📊 Summary:")
    print(f"  Hyperliquid unmapped: {len(hyperliquid_set)}")
    print(f"  Lighter unmapped: {len(lighter_set)}")
    print(f"  Unmapped in BOTH: {len(both_unmapped)}")
    print(f"  Only Hyperliquid: {len(only_hyperliquid)}")
    print(f"  Only Lighter: {len(only_lighter)}")
    print()

    print("=" * 80)
    print(f"❌ TOKENS UNMAPPED IN BOTH EXCHANGES ({len(both_unmapped)} tokens)")
    print("=" * 80)
    print("These tokens cannot be mapped from EITHER Hyperliquid OR Lighter:")
    print()
    for token in sorted(both_unmapped):
        print(f"  • {token}")
    print()

    print("=" * 80)
    print(f"🔵 ONLY HYPERLIQUID UNMAPPED ({len(only_hyperliquid)} tokens)")
    print("=" * 80)
    print("These tokens are unmapped on Hyperliquid but might be on Lighter:")
    print()
    for token in sorted(only_hyperliquid):
        print(f"  • {token}")
    print()

    print("=" * 80)
    print(f"🟢 ONLY LIGHTER UNMAPPED ({len(only_lighter)} tokens)")
    print("=" * 80)
    print("These tokens are unmapped on Lighter but might be on Hyperliquid:")
    print()
    for token in sorted(only_lighter):
        print(f"  • {token}")
    print()

    # Additional insights
    print("=" * 80)
    print("🔍 KEY INSIGHTS")
    print("=" * 80)
    print()
    print(f"1. {len(both_unmapped)} tokens ({len(both_unmapped)/len(hyperliquid_set | lighter_set)*100:.1f}% of all unmapped) are problematic")
    print("   → These need on-chain price discovery (V2/V3 pools)")
    print()
    print(f"2. {len(only_hyperliquid)} tokens can potentially use Lighter prices")
    print(f"3. {len(only_lighter)} tokens can potentially use Hyperliquid prices")
    print()

    # Notable major tokens in both_unmapped
    major_tokens = {
        "BTC", "ETH", "USDC", "USDT", "DAI", "WBTC", "WETH",
        "UNI", "AAVE", "LINK", "CRV", "MKR", "SNX", "COMP"
    }
    major_unmapped = both_unmapped & major_tokens

    if major_unmapped:
        print("⚠️  CRITICAL: Major tokens unmapped in both exchanges:")
        for token in sorted(major_unmapped):
            print(f"   • {token}")
        print()

    return {
        "both_unmapped": sorted(both_unmapped),
        "only_hyperliquid": sorted(only_hyperliquid),
        "only_lighter": sorted(only_lighter),
        "summary": {
            "total_hyperliquid": len(hyperliquid_set),
            "total_lighter": len(lighter_set),
            "both_unmapped_count": len(both_unmapped),
            "only_hyperliquid_count": len(only_hyperliquid),
            "only_lighter_count": len(only_lighter),
        },
    }


if __name__ == "__main__":
    result = analyze_unmapped_tokens()

    # Save results
    output_path = Path(
        "/home/sam-sullivan/dynamicWhitelist/data/unmapped_tokens/overlap_analysis.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"✅ Results saved to: {output_path}")
