# ConnectAPI

A FastAPI-based API service for accessing BRACU (BRAC University) student schedules. This API provides a simplified interface to fetch schedule data from the BRACU Connect portal.

## Features

- Public schedule endpoint
- Token management and auto-refresh
- Upstash Redis for token storage and caching
- Error handling and fallback mechanisms
- Deployed on Vercel
- **Session-based security for sensitive endpoints**: Only users with a valid session can access `/enter-tokens` and `/mytokens`. The `/raw-schedule` endpoint remains public.
- **Token uptime display**: The home page shows how long your current token will remain active (if any).
- **Optimized Lab Section Handling**: 
  - Smart caching of lab section data in Redis
  - Direct merging of child sections without suffix checks
  - Reduced API calls by filtering theory and other sections
  - Real-time updates for new lab sections

## Tech Stack

- **Backend Framework**: FastAPI
- **Database**: Upstash Redis (Serverless Redis)
- **Deployment**: Vercel
- **Runtime**: Python 3.7+

## Setup

1. Clone the repository:
```bash
git clone https://github.com/cswasif/ConnectAPI.git
cd ConnectAPI
```

2. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Set up Upstash Redis:
- Go to [Upstash Console](https://console.upstash.com/)
- Create a new Redis database
- Copy the connection details

4. Create a .env file with your configuration:
```env
# Upstash Redis Configuration
REDIS_URL=rediss://default:your-password@willing-husky-43244.upstash.io:6379
```

5. Run the server locally:
```bash
uvicorn main:app --reload
```

## Deployment

This project is configured for deployment on Vercel:

1. Fork this repository
2. Connect your fork to Vercel
3. Add your environment variables in Vercel:
   - `REDIS_URL` (from Upstash)
4. Deploy!

## API Endpoints

### GET /raw-schedule
Fetches the current schedule using the most recent valid token, or falls back to the latest cached schedule if no valid token is available.

- No authentication required
- Returns schedule data in JSON format
- Uses Upstash Redis for token management and caching
- **How it works:**
  - The endpoint always tries to fetch the latest real schedule from the Connect API using the most recent valid token.
  - If the live API call is successful, it updates the cache and returns the fresh data.
  - If the live API call fails (e.g., network error, token expired), it falls back to the most recently cached schedule (if available).
  - **If no valid token is available at all, it will show the most recently cached schedule (from any student) if present.**
  - If neither a valid token nor any cached schedule exists, an error is returned.
- **Lab Section Handling:**
  - Automatically detects and merges child sections (labs) with their parent sections
  - Uses local caching in `connect.json` to minimize API calls
  - Only checks theory sections for potential lab sections
  - Updates cache when new lab sections are found
- **Everyone sees the same schedule**—it is not user-specific.

### GET/POST /enter-tokens
Allows users to enter and save their access and refresh tokens.

- **Requires a valid session** (users must start from the home page to get a session)
- Tokens are stored securely in Redis, scoped to the session
- Not accessible to the public without a session

### GET /mytokens
View the tokens associated with the current session.

- **Requires a valid session**
- Not accessible to the public without a session

## Live API URLs & Usage

- **Base URL:** https://connapi.vercel.app
- **Raw Schedule Endpoint:** https://connapi.vercel.app/raw-schedule

### How to Use

- Visit [https://connapi.vercel.app/raw-schedule](https://connapi.vercel.app/raw-schedule) in your browser or use it in your application to fetch the latest available schedule data.
- If a valid token has been submitted (via the web UI at `/enter-tokens`), the endpoint will show the latest real schedule from the BRACU Connect API.
- If no valid token is available, it will show the most recently cached schedule (if any exists).
- The endpoint returns data in JSON format and is public—no authentication is required.
- **The home page also displays the remaining active time (uptime) of your current token, if available.**

## Dependencies

- Python 3.7+
- FastAPI
- Upstash Redis
- httpx
- python-jose[cryptography]

## Infrastructure

### Upstash Redis
We use Upstash Redis as our primary database for:
- Token storage and management
- Schedule data caching
- Lab section caching
- Session handling

Benefits of using Upstash:
- Serverless Redis solution
- Global data replication
- Pay-per-use pricing
- Built-in REST API

### Vercel
Our deployment platform offering:
- Serverless Functions
- Automatic deployments
- Edge Network
- Zero configuration

## Performance Optimizations

### Lab Section Handling
The API includes several optimizations for handling lab sections:

1. **Smart Caching**:
   - Lab section data is cached in Redis
   - Reduces unnecessary API calls for known lab sections
   - Cache is updated when new lab sections are discovered
   - Persistent across serverless function invocations

2. **Efficient Child Section Merging**:
   - Directly merges child section data with parent sections
   - No unnecessary suffix checks or filtering
   - Preserves all relevant lab information

3. **API Call Reduction**:
   - Only checks theory and other sections for potential labs
   - Skips sections that are already labs
   - Uses cached data whenever possible

4. **Real-time Updates**:
   - Updates cache when new lab sections are found
   - Maintains cache consistency across requests
   - Minimizes data staleness

## Credits & Purpose

This API server was developed by **Wasif Faisal** to support [Routinez](https://routinez.vercel.app/) and other BRACU student tools.

## License

MIT License 