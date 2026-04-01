import shutil
import os
import gzip
import struct
from datetime import datetime
import glob

# Constants
CHUNK_SIZE = 4096  # Standard SQLite page size
FULL_BACKUP_PREFIX = "noc_data_FULL_"
PATCH_BACKUP_PREFIX = "noc_data_PATCH_"
RETENTION_DAYS = 15

def get_latest_full_backup(backup_dir):
    """Finds the most recent FULL backup file."""
    pattern = os.path.join(backup_dir, f"{FULL_BACKUP_PREFIX}*.db.gz")
    files = glob.glob(pattern)
    if not files:
        return None
    # Sort by filename (which includes timestamp)
    files.sort(reverse=True)
    return files[0]

def create_full_backup(db_path, backup_dir, timestamp):
    """Creates a compressed full copy of the database."""
    backup_filename = f"{FULL_BACKUP_PREFIX}{timestamp}.db.gz"
    backup_path = os.path.join(backup_dir, backup_filename)
    
    print(f"Creating FULL backup: {backup_filename}")
    with open(db_path, 'rb') as f_in:
        with gzip.open(backup_path, 'wb', compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    return backup_path

def create_patch_backup(db_path, full_backup_path, backup_dir, timestamp):
    """Creates a compressed patch relative to a full backup."""
    patch_filename = f"{PATCH_BACKUP_PREFIX}{timestamp}.patch.gz"
    patch_path = os.path.join(backup_dir, patch_filename)
    
    print(f"Creating DIFFERENTIAL backup from: {os.path.basename(full_backup_path)}")
    
    # Store the reference to the full backup at the start of the patch
    ref_name = os.path.basename(full_backup_path).encode('utf-8')
    
    changed_count = 0
    with open(db_path, 'rb') as f_curr:
        with gzip.open(full_backup_path, 'rb') as f_base:
            with gzip.open(patch_path, 'wb', compresslevel=6) as f_patch:
                # Header: Reference Full Backup Name
                f_patch.write(struct.pack("<I", len(ref_name)))
                f_patch.write(ref_name)
                
                # Compare chunks
                offset = 0
                while True:
                    c_curr = f_curr.read(CHUNK_SIZE)
                    c_base = f_base.read(CHUNK_SIZE)
                    
                    if not c_curr:
                        break
                    
                    # If current is longer than base, assume base is 0-padded at end
                    if not c_base:
                        c_base = b'\x00' * len(c_curr)
                    
                    if c_curr != c_base:
                        # Write [OFFSET(8)][DATA(CHUNK)]
                        f_patch.write(struct.pack("<Q", offset))
                        f_patch.write(c_curr)
                        changed_count += 1
                        
                    offset += len(c_curr)
    
    print(f"Differential backup complete. Changed {changed_count} pages (~{changed_count*CHUNK_SIZE/1024:.1f} KB raw diff).")
    return patch_path

def create_db_backup(db_path='noc_data.db', backup_dir='backups'):
    """
    Creates a backup of the database. 
    Uses full backups once a day and differential patches in between.
    """
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found. Nothing to backup.")
        return None

    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
        print(f"Created backup directory: {backup_dir}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    latest_full = get_latest_full_backup(backup_dir)
    
    # Logic: If no full backup exists, or the last full is > 24 hours old, make a new full.
    should_do_full = True
    if latest_full:
        full_mtime = os.path.getmtime(latest_full)
        if (datetime.now().timestamp() - full_mtime) < (24 * 60 * 60):
            should_do_full = False

    try:
        if should_do_full:
            result_path = create_full_backup(db_path, backup_dir, timestamp)
        else:
            result_path = create_patch_backup(db_path, latest_full, backup_dir, timestamp)
            
        # Cleanup old backups
        cutoff = datetime.now().timestamp() - (RETENTION_DAYS * 24 * 60 * 60)
        
        # New patterns
        patterns = [
            f"{FULL_BACKUP_PREFIX}*",
            f"{PATCH_BACKUP_PREFIX}*",
            "noc_data_backup_*.db" # Old full-backup format
        ]
        
        all_backups = []
        for p in patterns:
            all_backups += glob.glob(os.path.join(backup_dir, p))
        
        removed_count = 0
        for b in all_backups:
            if os.path.exists(b) and os.path.getmtime(b) < cutoff:
                os.remove(b)
                removed_count += 1
        
        if removed_count > 0:
            print(f"Cleaned up {removed_count} old backups.")
            
        return result_path
    except Exception as e:
        print(f"Backup failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    create_db_backup()
