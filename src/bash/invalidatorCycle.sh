# Set up the environment
source /data/unifiedPy3/setUnifiedPy3Env.sh

# Checkout to the proper branch
git checkout python3-migration

# Set up the permissions
chmod +x src/bash/*.sh

# Run the script
source src/bash/runner.sh src/python/Unified/Invalidator.py