import os
import gzip
import struct
import shutil
import sys

CHUNK_SIZE = 4096

def restore_db(patch_path, output_db_path='noc_data_restored.db'):
    """Reconstructs the database from a patch file and its base full backup."""
    if not os.path.exists(patch_path):
        print(f"Error: Patch file {patch_path} not found.")
        return

    backup_dir = os.path.dirname(patch_path)
    
    try:
        with gzip.open(patch_path, 'rb') as f_patch:
            # 1. Read Header (Reference Full Backup Name)
            header_len_data = f_patch.read(4)
            if not header_len_data:
                 print("Error: Invalid patch file header.")
                 return
            header_len = struct.unpack("<I", header_len_data)[0]
            ref_name = f_patch.read(header_len).decode('utf-8')
            
            full_backup_path = os.path.join(backup_dir, ref_name)
            if not os.path.exists(full_backup_path):
                 print(f"Error: Base full backup not found at {full_backup_path}")
                 return
            
            print(f"Restoring from: {ref_name}")
            print(f"Target: {output_db_path}")
            
            # 2. Decompress FULL backup to output file
            print("Decompressing base backup...")
            with gzip.open(full_backup_path, 'rb') as f_base:
                with open(output_db_path, 'wb') as f_out:
                    shutil.copyfileobj(f_base, f_out)
            
            # 3. Apply Patches
            print("Applying patches...")
            applied_count = 0
            with open(output_db_path, 'r+b') as f_out:
                while True:
                    offset_data = f_patch.read(8)
                    if not offset_data:
                        break
                    
                    offset = struct.unpack("<Q", offset_data)[0]
                    page_data = f_patch.read(CHUNK_SIZE)
                    
                    f_out.seek(offset)
                    f_out.write(page_data)
                    applied_count += 1
            
            print(f"Successfully restored! Applied {applied_count} changed pages.")
            print(f"Restored file size: {os.path.getsize(output_db_path) / (1024*1024):.2f} MB")

    except Exception as e:
        print(f"Restore failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/restore_db.py <path_to_patch_file> [output_db_path]")
    else:
        patch = sys.argv[1]
        out = sys.argv[2] if len(sys.argv) > 2 else 'noc_data_restored.db'
        restore_db(patch, out)
