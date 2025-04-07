import folium
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from folium.plugins import MarkerCluster
from collections import defaultdict
from openrouteservice import Client
import time

# Configuration
ORS_API_KEY = "5b3ce3597851110001cf62487128e89e37864b1c8bb039694215c48b"
SHEET_NAME = "delivery_orders"
SERVICE_ACCOUNT_FILE = "service_account.json"
darkstore_coords = (12.9093, 77.6483)
REFRESH_INTERVAL = 30  # seconds


# Google Sheets Setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1


# Setup ORS client
ors = Client(key=ORS_API_KEY)

print("Real-time map generation started. Refreshing every 30 seconds...")

while True:
    try:
        # Fetch latest order data
        data = sheet.get_all_records()

        # Initialize map
        m = folium.Map(location=darkstore_coords, zoom_start=15)

        # Auto-refresh HTML using JavaScript
        m.get_root().html.add_child(folium.Element(f"""
            <script>
                setTimeout(function() {{
                    location.reload();
                }}, {REFRESH_INTERVAL * 1000});
            </script>
        """))

        # Add darkstore marker
        folium.Marker(darkstore_coords, tooltip="Dark Store", icon=folium.Icon(color="black")).add_to(m)

        # Partner colors and routes
        partner_colors = ["red", "blue", "green", "purple", "orange", "pink", "cadetblue", "darkred", "beige", "darkgreen"]
        partner_routes = defaultdict(list)

        # Process orders
        for order in data:
            try:
                order_id = order["Order ID"]
                lat = float(order["Latitude"])
                lng = float(order["Longitude"])
                status = order.get("Status", "").lower()
                partner = order.get("Assigned Partner", "None")
                eta = order.get("ETA (mins)", "Unknown")
                return_eta = order.get("Return ETA (mins)", "Unknown")

                # Marker color based on status
                if status == "delivered":
                    marker_color = "green"
                elif status == "in_transit":
                    marker_color = "yellow"
                elif status == "unassigned":
                    marker_color = "red"
                else:
                    marker_color = "gray"

                popup_text = f"""
                <b>Order ID:</b> {order_id}<br>
                <b>Status:</b> {status}<br>
                <b>Assigned Partner:</b> {partner}<br>
                <b>ETA:</b> {eta} mins<br>
                <b>Return ETA:</b> {return_eta} mins
                """

                folium.Marker(
                    location=(lat, lng),
                    popup=popup_text,
                    icon=folium.Icon(color=marker_color),
                ).add_to(m)

                if partner and partner.lower() != "none":
                    partner_routes[partner].append((lng, lat))  # ORS expects (lng, lat)
            except Exception as e:
                print(f"Skipping order due to error: {e}")

        # Draw partner routes
        for idx, (partner, waypoints) in enumerate(partner_routes.items()):
            route_coords = [(darkstore_coords[1], darkstore_coords[0])] + waypoints + [(darkstore_coords[1], darkstore_coords[0])]
            try:
                route = ors.directions(
                    coordinates=route_coords,
                    profile='driving-car',
                    format='geojson'
                )
                folium.PolyLine(
                    locations=[(pt[1], pt[0]) for pt in route['features'][0]['geometry']['coordinates']],
                    color=partner_colors[idx % len(partner_colors)],
                    weight=3,
                    tooltip=f"Partner {partner} Route"
                ).add_to(m)
            except Exception as e:
                print(f"Failed to get route for Partner {partner}: {e}")

        # Save the map
        m.save("delivery_map.html")
        print("Map updated at", time.strftime("%Y-%m-%d %H:%M:%S"))

    except Exception as err:
        print(f"Error updating map: {err}")

    time.sleep(REFRESH_INTERVAL)
