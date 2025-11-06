# --- Python 3.13 compatibility patch ---
# Fixes: ModuleNotFoundError: No module named 'imghdr'
try:
    import imghdr  # Python <3.13 still has it
except ModuleNotFoundError:
    import types
    imghdr = types.ModuleType("imghdr")
    imghdr.what = lambda file=None, h=None: None

# Export imghdr in case modules import it from this package
__all__ = ["imghdr"]
