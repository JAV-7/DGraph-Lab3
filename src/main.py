#!/usr/bin/env python3
"""
@file: main.py
@author: Francisco Javier Ramos Jimenez.
@date: 23/11/2025.
@description: Main application file for Dgraph video platform database management.
"""

import os
import pydgraph
import model
import json

"""
Display main menu options.
"""
def print_menu():
    print("\n" + "="*60)
    mm_options = {
        1: "Create/Load Data (from CSV files)",
        2: "Query 1: Text search",
        3: "Query 2: Numeric duration",
        4: "Query 3: Users with posts",
        5: "Query 4: Video posters reverse",
        6: "Query 5: Videos sorted",
        7: "Query 6: Video count",
        8: "Query 7: Video pages",
        9: "Delete comments by term",
        10: "Drop all",
        11: "Exit"
    }
    for key in mm_options.keys():
        print(f"  {key} -- {mm_options[key]}")
    print("="*60)

"""
Create and return a Dgraph client stub.
Returns:
    DGraphClientStub
"""
def create_client_stub():
    return pydgraph.DgraphClientStub("localhost:9080")

"""
Create and return a Dgraph client.
Returns:
    DGraphClient
"""
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

"""
Close the client stub connection.
"""
def close_client_stub(client_stub):
    client_stub.close()

"""
Main application loop.
"""
def main():
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)
    print("Connecting to Dgraph...")
    
    # Create schema
    try:
        print("Setting up database schema...")
        model.set_schema(client)
        print("Schema created successfully!")
    except Exception as e:
        print(f"Warning: Could not set schema: {e}")
    
    # Main menu loop
    while True:
        try:
            print_menu()
            option = input('\nEnter your choice: ').strip()
            
            try:
                option = int(option)
            except Exception as e:
                print("Invalid input! Please, try again :)")
                continue
            
            if option == 1:
                # Create/Load Data
                print("\n--- LOADING DATA FROM CSV FILES ---")
                model.create_data(client)
            
            elif option == 2:
                # Query 1: Text Search
                text_search = input("Enter text search: ")
                results = model.query_by_text(client, text_search)
                if results:
                    print(json.dumps(results, indent=2))
                else:
                    print("No comments found for that search.")
            
            elif option == 3:
               # Query 2: Query by numeric duration
               min_duration = int(input("Enter minimum video duration: "))
               results = model.query_by_numeric_duration(client, min_duration)
               if results:
                   print(json.dumps(results, indent=2))
               else:
                   print("No videos match the duration filter.")
            
            elif option == 4:
                # Query 3: Query users with posts
                results = model.query_users_with_posts(client)
                if results:
                    print(json.dumps(results, indent=2))
                else:
                    print("No users with posts found.")
            
            elif option == 5:
                # Query 4: query video posters reverse
                video_title = input("Enter video title: ")
                results = model.query_video_posters_reverse(client, video_title)
                if results:
                    print(json.dumps(results, indent=2))
                else:
                    print("No posters found for that video.")
            
            elif option == 6:
                # Query 5: Query videos sorted
                sort_by = input("Enter 'desc' or 'asc', depending on the result you want: ")
                desc = sort_by.strip().lower() != 'asc'
                results = model.query_videos_sorted(client, desc)
                if results:
                    print(json.dumps(results, indent=2))
                else:
                    print("No videos found.")
            
            elif option == 7:
                # Query 6: Query video count
                count = model.query_video_count(client)
                print(f"Video count: {count}")

            elif option == 8:
                # Query 7: Query videos pages
                first = input("Enter first result: ")
                offset = input("Enter offset: ")
                try:
                    first_i = int(first)
                    offset_i = int(offset)
                except Exception:
                    print("Invalid pagination values; using defaults.")
                    first_i = 2
                    offset_i = 0
                results = model.query_videos_paged(client, first_i, offset_i)
                if results:
                    print(json.dumps(results, indent=2))
                else:
                    print("No videos on that page.")

            elif option == 9:
                comment_text = input("Enter comment text: ")
                model.delete_comment(client, comment_text)

            elif option == 10:
                model.drop_all(client)
                print("All data and schema dropped.")
            
            elif option == 11:
                # Exit
                print("\nExiting application...")
                model.drop_all(client)
                print("All data dropped.")
                close_client_stub(client_stub)
                print("Goodbye!")
                exit(0)
            
            else:
                print("Invalid option! Please choose a number between 1 and 8.")
            
            # Pause before showing menu again
            input("\nPress Enter to continue...")
        
        except Exception as e:
            print(f'\nError: {e}')
            print("Please try again.")
            input("\nPress Enter to continue...")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))