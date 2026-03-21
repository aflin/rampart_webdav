#!/bin/sh
#
# Administer the Rampart File Manager.
#
# From outside Docker:
#   docker exec <container> /app/admin.sh <command> [args...]
#
# From inside Docker:
#   /app/admin.sh <command> [args...]
#
# Commands:
#   add <username> <password>      Create a user
#   del <username>                 Delete a user
#   list                           List all users
#   passwd <username> <password>   Change password
#   admin <username> true|false    Set admin status
#

if [ $# -eq 0 ]; then
    echo "Usage: $0 <command> [args...]"
    echo ""
    echo "Commands:"
    echo "  add <username> <password>      Create a user"
    echo "  del <username>                 Delete a user"
    echo "  list                           List all users"
    echo "  passwd <username> <password>   Change password"
    echo "  admin <username> true|false    Set admin status"
    exit 1
fi

cd /app
rampart apps/webdav/webdav.js "$@"
