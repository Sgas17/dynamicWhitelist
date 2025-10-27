"""
JSON file storage implementation for backups and exports.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime
import gzip
import shutil

from .base import StorageBase, DataError

logger = logging.getLogger(__name__)


class JsonStorage(StorageBase):
    """
    JSON file storage implementation for data persistence and backups.
    
    Features:
    - Save/load JSON files
    - Compression support
    - Atomic writes
    - Backup rotation
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize JSON storage.
        
        Args:
            config: Configuration with keys:
                - base_path: Base directory for JSON storage
                - compress: Whether to compress files (default: False)
                - pretty: Whether to pretty-print JSON (default: True)
                - backup_count: Number of backups to keep (default: 5)
        """
        super().__init__(config)
        self.base_path = Path(config.get('base_path', './data'))
        self.compress = config.get('compress', False)
        self.pretty = config.get('pretty', True)
        self.backup_count = config.get('backup_count', 2)
        
    async def connect(self) -> None:
        """Ensure base directory exists."""
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.is_connected = True
        logger.info(f"JSON storage initialized at {self.base_path}")
        
    async def disconnect(self) -> None:
        """No-op for JSON storage."""
        self.is_connected = False
        
    async def health_check(self) -> bool:
        """Check if base directory is accessible."""
        return self.base_path.exists() and self.base_path.is_dir()
        
    def _get_full_path(self, filename: str) -> Path:
        """
        Get full path for a file.
        
        Args:
            filename: Relative filename
            
        Returns:
            Full path object
        """
        if not filename.endswith('.json') and not filename.endswith('.json.gz'):
            filename = f"{filename}.json"
            
        if self.compress and not filename.endswith('.gz'):
            filename = f"{filename}.gz"
            
        return self.base_path / filename
        
    def save(self, filename: str, data: Any) -> bool:
        """
        Save data to a JSON file.
        
        Args:
            filename: File name (relative to base_path)
            data: Data to save
            
        Returns:
            bool: True if successful
        """
        try:
            filepath = self._get_full_path(filename)
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Atomic write with temporary file
            temp_path = filepath.with_suffix('.tmp')
            
            if self.compress:
                with gzip.open(temp_path, 'wt', encoding='utf-8') as f:
                    if self.pretty:
                        json.dump(data, f, indent=2, default=str)
                    else:
                        json.dump(data, f, default=str)
            else:
                with open(temp_path, 'w', encoding='utf-8') as f:
                    if self.pretty:
                        json.dump(data, f, indent=2, default=str)
                    else:
                        json.dump(data, f, default=str)
                        
            # Move temp file to final location
            temp_path.rename(filepath)
            
            logger.info(f"Saved data to {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save JSON file {filename}: {e}")
            raise DataError(f"JSON save failed: {e}")
            
    def load(self, filename: str) -> Optional[Any]:
        """
        Load data from a JSON file.
        
        Args:
            filename: File name (relative to base_path)
            
        Returns:
            Loaded data or None if file doesn't exist
        """
        try:
            filepath = self._get_full_path(filename)
            
            if not filepath.exists():
                # Try without compression extension
                filepath_no_gz = filepath.with_suffix('')
                if filepath_no_gz.exists():
                    filepath = filepath_no_gz
                else:
                    return None
                    
            if filepath.suffix == '.gz':
                with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
            logger.info(f"Loaded data from {filepath}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to load JSON file {filename}: {e}")
            raise DataError(f"JSON load failed: {e}")
            
    def save_backup(self, filename: str, data: Any) -> bool:
        """
        Save data with automatic backup rotation.
        
        Args:
            filename: File name
            data: Data to save
            
        Returns:
            bool: True if successful
        """
        try:
            filepath = self._get_full_path(filename)
            
            # Rotate existing backups
            if filepath.exists():
                self._rotate_backups(filepath)
                
            # Save new data
            return self.save(filename, data)
            
        except Exception as e:
            logger.error(f"Failed to save backup {filename}: {e}")
            raise DataError(f"Backup save failed: {e}")
            
    def load_backup(self, filename: str, backup_index: int = 0) -> Optional[Any]:
        """
        Load data from a backup file.
        
        Args:
            filename: File name
            backup_index: Which backup to load (0 = current, 1 = most recent backup)
            
        Returns:
            Loaded data or None if backup doesn't exist
        """
        try:
            if backup_index == 0:
                return self.load(filename)
                
            filepath = self._get_full_path(filename)
            backup_path = filepath.with_suffix(f'.backup{backup_index}{filepath.suffix}')
            
            if not backup_path.exists():
                return None
                
            # Load backup file
            backup_filename = str(backup_path.relative_to(self.base_path))
            return self.load(backup_filename)
            
        except Exception as e:
            logger.error(f"Failed to load backup {filename}: {e}")
            raise DataError(f"Backup load failed: {e}")
            
    def _rotate_backups(self, filepath: Path) -> None:
        """
        Rotate backup files, keeping only backup_count versions.
        
        Args:
            filepath: Path to the main file
        """
        # Move existing backups
        for i in range(self.backup_count - 1, 0, -1):
            old_backup = filepath.with_suffix(f'.backup{i}{filepath.suffix}')
            new_backup = filepath.with_suffix(f'.backup{i+1}{filepath.suffix}')
            
            if old_backup.exists():
                if new_backup.exists():
                    new_backup.unlink()
                old_backup.rename(new_backup)
                
        # Move current file to backup1
        backup1 = filepath.with_suffix(f'.backup1{filepath.suffix}')
        if filepath.exists():
            if backup1.exists():
                backup1.unlink()
            shutil.copy2(filepath, backup1)
            
    # Whitelist-specific methods
    
    def save_whitelist(self, chain: str, whitelist: List[Dict[str, Any]]) -> bool:
        """
        Save whitelist data for a specific chain.
        
        Args:
            chain: Chain identifier
            whitelist: List of whitelisted tokens
            
        Returns:
            bool: True if successful
        """
        filename = f"whitelists/{chain}_whitelist.json"
        
        data = {
            'chain': chain,
            'timestamp': datetime.utcnow().isoformat(),
            'count': len(whitelist),
            'tokens': whitelist
        }
        
        return self.save_backup(filename, data)
        
    def load_whitelist(self, chain: str) -> Optional[List[Dict[str, Any]]]:
        """
        Load whitelist data for a specific chain.
        
        Args:
            chain: Chain identifier
            
        Returns:
            List of whitelisted tokens or None
        """
        filename = f"whitelists/{chain}_whitelist.json"
        data = self.load(filename)
        
        if data and 'tokens' in data:
            return data['tokens']
            
        return None
        
    # Pool-specific methods
    
    def save_pools(self, chain: str, protocol: str, pools: List[Dict[str, Any]]) -> bool:
        """
        Save pool data for a specific chain and protocol.
        
        Args:
            chain: Chain identifier
            protocol: Protocol identifier
            pools: List of pools
            
        Returns:
            bool: True if successful
        """
        filename = f"pools/{chain}_{protocol}_pools.json"
        
        data = {
            'chain': chain,
            'protocol': protocol,
            'timestamp': datetime.utcnow().isoformat(),
            'count': len(pools),
            'pools': pools
        }
        
        return self.save_backup(filename, data)
        
    def load_pools(self, chain: str, protocol: str) -> Optional[List[Dict[str, Any]]]:
        """
        Load pool data for a specific chain and protocol.
        
        Args:
            chain: Chain identifier
            protocol: Protocol identifier
            
        Returns:
            List of pools or None
        """
        filename = f"pools/{chain}_{protocol}_pools.json"
        data = self.load(filename)
        
        if data and 'pools' in data:
            return data['pools']
            
        return None
        
    # Batch operations
    
    def save_all_whitelists(self, whitelists: Dict[str, List[Dict[str, Any]]]) -> Dict[str, bool]:
        """
        Save whitelists for all chains.
        
        Args:
            whitelists: Dictionary mapping chain to whitelist
            
        Returns:
            Dictionary mapping chain to success status
        """
        results = {}
        
        for chain, whitelist in whitelists.items():
            try:
                results[chain] = self.save_whitelist(chain, whitelist)
            except Exception as e:
                logger.error(f"Failed to save whitelist for {chain}: {e}")
                results[chain] = False
                
        return results
        
    def load_all_whitelists(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Load whitelists for all available chains.
        
        Returns:
            Dictionary mapping chain to whitelist
        """
        whitelists = {}
        whitelist_dir = self.base_path / 'whitelists'
        
        if not whitelist_dir.exists():
            return whitelists
            
        for filepath in whitelist_dir.glob('*_whitelist.json*'):
            # Extract chain name from filename
            chain = filepath.stem.replace('_whitelist', '')
            if chain.endswith('.json'):  # Handle .json.gz case
                chain = chain[:-5]
                
            whitelist = self.load_whitelist(chain)
            if whitelist:
                whitelists[chain] = whitelist
                
        return whitelists
        
    # Export/Import methods
    
    def export_data(self, export_file: str, include_whitelists: bool = True, 
                   include_pools: bool = True) -> bool:
        """
        Export all data to a single file.
        
        Args:
            export_file: Export file name
            include_whitelists: Whether to include whitelists
            include_pools: Whether to include pools
            
        Returns:
            bool: True if successful
        """
        try:
            export_data = {
                'export_time': datetime.utcnow().isoformat(),
                'version': '1.0'
            }
            
            if include_whitelists:
                export_data['whitelists'] = self.load_all_whitelists()
                
            if include_pools:
                pools = {}
                pool_dir = self.base_path / 'pools'
                
                if pool_dir.exists():
                    for filepath in pool_dir.glob('*_pools.json*'):
                        # Extract chain and protocol from filename
                        parts = filepath.stem.replace('_pools', '').split('_')
                        if len(parts) >= 2:
                            key = f"{parts[0]}_{parts[1]}"
                            data = self.load(f"pools/{filepath.name}")
                            if data and 'pools' in data:
                                pools[key] = data['pools']
                                
                export_data['pools'] = pools
                
            return self.save(export_file, export_data)
            
        except Exception as e:
            logger.error(f"Failed to export data: {e}")
            raise DataError(f"Export failed: {e}")
            
    def import_data(self, import_file: str) -> bool:
        """
        Import data from an export file.
        
        Args:
            import_file: Import file name
            
        Returns:
            bool: True if successful
        """
        try:
            data = self.load(import_file)
            
            if not data:
                return False
                
            # Import whitelists
            if 'whitelists' in data:
                for chain, whitelist in data['whitelists'].items():
                    self.save_whitelist(chain, whitelist)
                    
            # Import pools
            if 'pools' in data:
                for key, pools in data['pools'].items():
                    parts = key.split('_')
                    if len(parts) >= 2:
                        self.save_pools(parts[0], parts[1], pools)
                        
            logger.info(f"Imported data from {import_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import data: {e}")
            raise DataError(f"Import failed: {e}")