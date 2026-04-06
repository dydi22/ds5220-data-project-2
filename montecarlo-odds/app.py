import io
import os
from datetime import datetime, timezone

import boto3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import requests
from botocore.exceptions import ClientError


ODDS_API_KEY = os.environ["ODDS_API_KEY"]
S3_BUCKET = os.environ["S3_BUCKET"]

SPORT_KEY = os.getenv("SPORT_KEY", "golf_masters_tournament_winner")
ODDS_REGIONS = os.getenv("ODDS_REGIONS", "uk")
S3_DATA_KEY = os.getenv("S3_DATA_KEY", "data.csv")
S3_PLOT_KEY = os.getenv("S3_PLOT_KEY", "plot.png")

ODDS_URL = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/odds"
TIMEOUT_SECONDS = 30

s3 = boto3.client("s3")


def fetch_odds():
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": "outrights",
        "oddsFormat": "decimal",
    }
    response = requests.get(ODDS_URL, params=params, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def load_existing_history():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_DATA_KEY)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"NoSuchKey", "404", "NotFound"}:
            return pd.DataFrame()
        raise


def extract_player_odds(events):
    """
    Build one row per player for the current snapshot.
    If multiple bookmakers are present, average the decimal odds by player.
    """
    snapshot_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    player_prices = {}

    for event in events:
        for bookmaker in event.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                if market.get("key") != "outrights":
                    continue

                for outcome in market.get("outcomes", []):
                    name = outcome.get("name")
                    price = outcome.get("price")

                    if name is None or price is None:
                        continue

                    try:
                        price = float(price)
                    except (TypeError, ValueError):
                        continue

                    if price <= 0:
                        continue

                    player_prices.setdefault(name, []).append(price)

    rows = []
    for player, prices in player_prices.items():
        avg_price = sum(prices) / len(prices)
        raw_implied_prob = 1.0 / avg_price
        rows.append(
            {
                "snapshot_time_utc": snapshot_time,
                "player_name": player,
                "decimal_odds": round(avg_price, 4),
                "raw_implied_prob": raw_implied_prob,
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    total_raw = df["raw_implied_prob"].sum()
    df["implied_prob_normalized"] = df["raw_implied_prob"] / total_raw
    df["implied_prob_normalized"] = df["implied_prob_normalized"].round(6)

    df = df.sort_values("implied_prob_normalized", ascending=False).reset_index(drop=True)
    return df


def build_plot(history_df):
    plot_df = history_df.copy()
    plot_df["snapshot_time_utc"] = pd.to_datetime(plot_df["snapshot_time_utc"], utc=True, errors="coerce")
    plot_df = plot_df.dropna(subset=["snapshot_time_utc"])

    if plot_df.empty:
        return

    latest_time = plot_df["snapshot_time_utc"].max()
    latest_snapshot = plot_df[plot_df["snapshot_time_utc"] == latest_time].copy()
    top_players = (
        latest_snapshot
        .sort_values("implied_prob_normalized", ascending=False)
        .head(5)["player_name"]
        .tolist()
    )

    plot_df = plot_df[plot_df["player_name"].isin(top_players)].copy()

    fig, ax = plt.subplots(figsize=(11, 6))

    for player in top_players:
        player_df = plot_df[plot_df["player_name"] == player].sort_values("snapshot_time_utc")
        ax.plot(
            player_df["snapshot_time_utc"],
            player_df["implied_prob_normalized"],
            marker="o",
            label=player,
        )

    ax.set_title("Masters Tournament Winner Odds Over Time")
    ax.set_xlabel("Snapshot time (UTC)")
    ax.set_ylabel("Normalized implied probability")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.xticks(rotation=30)
    plt.tight_layout()
    fig.savefig("/tmp/plot.png", dpi=150)
    plt.close(fig)


def upload_outputs(history_df):
    csv_buffer = io.StringIO()
    history_df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_DATA_KEY,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    with open("/tmp/plot.png", "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_PLOT_KEY,
            Body=f.read(),
            ContentType="image/png",
        )


def main():
    events = fetch_odds()
    new_rows = extract_player_odds(events)

    if new_rows.empty:
        raise RuntimeError("No outright odds found in API response.")

    history = load_existing_history()

    if history.empty:
        combined = new_rows.copy()
    else:
        combined = pd.concat([history, new_rows], ignore_index=True)

    combined = combined.drop_duplicates(
        subset=["snapshot_time_utc", "player_name"], keep="last"
    ).sort_values(["snapshot_time_utc", "implied_prob_normalized"], ascending=[True, False])

    build_plot(combined)
    upload_outputs(combined)

    latest_time = combined["snapshot_time_utc"].max()
    latest_snapshot = combined[combined["snapshot_time_utc"] == latest_time].copy()
    latest_snapshot = latest_snapshot.sort_values("implied_prob_normalized", ascending=False).head(10)
    print(latest_snapshot.to_string(index=False))


if __name__ == "__main__":
    main()    return response.json()


def load_existing_history():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_DATA_KEY)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in {"NoSuchKey", "404", "NotFound"}:
            return pd.DataFrame()
        raise


def normalized_favorite_probability(outcomes):
    """
    outcomes: list like
    [{"name": "Player A", "price": 1.72}, {"name": "Player B", "price": 2.10}]
    Returns normalized favorite probability or None.
    """
    priced = []
    for outcome in outcomes:
        name = outcome.get("name")
        price = outcome.get("price")
        if name is None or price is None:
            continue
        try:
            price = float(price)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        priced.append((name, price))

    if len(priced) != 2:
        return None

    raw_probs = [1.0 / price for _, price in priced]
    total = sum(raw_probs)
    if total <= 0:
        return None

    normalized = [p / total for p in raw_probs]
    return max(normalized)


def summarize_snapshot(events):
    snapshot_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    favorite_probs = []
    bookmakers_per_match = []
    commence_times = []

    for event in events:
        per_match_bookmaker_probs = []

        for bookmaker in event.get("bookmakers", []):
            h2h_market = None
            for market in bookmaker.get("markets", []):
                if market.get("key") == "h2h":
                    h2h_market = market
                    break

            if h2h_market is None:
                continue

            favorite_prob = normalized_favorite_probability(h2h_market.get("outcomes", []))
            if favorite_prob is not None:
                per_match_bookmaker_probs.append(favorite_prob)

        if per_match_bookmaker_probs:
            favorite_probs.append(sum(per_match_bookmaker_probs) / len(per_match_bookmaker_probs))
            bookmakers_per_match.append(len(per_match_bookmaker_probs))

            commence_time = event.get("commence_time")
            if commence_time:
                commence_times.append(commence_time)

    summary = {
        "snapshot_time_utc": snapshot_time,
        "sport_key": SPORT_KEY,
        "api_event_count": len(events),
        "quoted_match_count": len(favorite_probs),
        "avg_favorite_prob": round(sum(favorite_probs) / len(favorite_probs), 4) if favorite_probs else None,
        "least_certain_favorite_prob": round(min(favorite_probs), 4) if favorite_probs else None,
        "most_lopsided_favorite_prob": round(max(favorite_probs), 4) if favorite_probs else None,
        "coinflip_match_count": sum(prob < 0.60 for prob in favorite_probs),
        "avg_bookmakers_per_match": round(sum(bookmakers_per_match) / len(bookmakers_per_match), 2) if bookmakers_per_match else None,
        "first_commence_time_utc": min(commence_times) if commence_times else None,
    }

    return pd.DataFrame([summary])


def build_plot(df):
    plot_df = df.copy()
    plot_df["snapshot_time_utc"] = pd.to_datetime(plot_df["snapshot_time_utc"], utc=True, errors="coerce")
    plot_df = plot_df.sort_values("snapshot_time_utc").dropna(subset=["snapshot_time_utc"])

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(
        plot_df["snapshot_time_utc"],
        plot_df["avg_favorite_prob"],
        marker="o",
        label="Average favorite win probability",
    )
    ax.plot(
        plot_df["snapshot_time_utc"],
        plot_df["least_certain_favorite_prob"],
        marker="o",
        label="Closest match favorite probability",
    )

    ax.set_title("ATP Monte-Carlo Masters market drift")
    ax.set_xlabel("Snapshot time (UTC)")
    ax.set_ylabel("Probability")
    ax.set_ylim(0.50, 1.00)
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.xticks(rotation=30)
    plt.tight_layout()
    fig.savefig("/tmp/plot.png", dpi=150)
    plt.close(fig)


def upload_outputs(history_df):
    csv_buffer = io.StringIO()
    history_df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_DATA_KEY,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    with open("/tmp/plot.png", "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_PLOT_KEY,
            Body=f.read(),
            ContentType="image/png",
        )


def main():
    events = fetch_odds()
    new_row = summarize_snapshot(events)
    history = load_existing_history()

    if history.empty:
        combined = new_row.copy()
    else:
        combined = pd.concat([history, new_row], ignore_index=True)

    combined = combined.drop_duplicates(subset=["snapshot_time_utc"], keep="last")
    combined = combined.sort_values("snapshot_time_utc")

    build_plot(combined)
    upload_outputs(combined)

    print(combined.tail(5).to_string(index=False))


if __name__ == "__main__":
    main()
