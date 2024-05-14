use std::collections::HashMap;
use std::fs::File;
use std::io::{Write};
use crates_io_api::{SyncClient, VersionDownloads};
use anyhow::Result;
use std::time::Duration;
use chrono::NaiveDate;
use log::info;
use ratelimit::Ratelimiter;
use reqwest::blocking::Client;
use reqwest::{header, Url};

const USER_AGENT: &str = "zip download stats (hennickc@amazon.com)";

fn main() -> Result<()> {
    let base_url = Url::parse("https://crates.io/api/v1/")?;
    simple_log::quick().expect("Failed to configure logging");
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::USER_AGENT,
        header::HeaderValue::from_str(USER_AGENT)?,
    );
    let raw_client = Client::builder()
        .default_headers(headers)
        .build()
        .unwrap();
    let client = SyncClient::new(USER_AGENT,
        Duration::from_secs(1))?;
    let mut out = File::create("../out.csv")?;
    let crate_info = client.full_crate("zip", true)?;
    let ratelimiter = Ratelimiter::builder(1, Duration::from_secs(1))
        .build()
        .unwrap();
    let mut semvers = HashMap::new();
    let mut total_downloads = HashMap::new();
    for version in &crate_info.versions {
        info!("Got version: {:?}", version);
        semvers.insert(version.id, version.num.clone());
        total_downloads.insert(version.num.clone(), version.downloads);
    }
    let mut downloads_by_version_and_date = HashMap::new();
    let mut start_date = NaiveDate::MAX;
    let mut end_date = NaiveDate::MIN;
    for version in &crate_info.versions {
        let version_link = base_url.join(&version.links.version_downloads)?;
        while let Err(sleep) = ratelimiter.try_wait() {
            std::thread::sleep(sleep);
        }
        let downloads_by_date: Box<[VersionDownloads]> = raw_client.get(version_link).send()?.json()?;
        for version_and_date in downloads_by_date.into_iter() {
            let date = version_and_date.date;
            start_date = start_date.min(date);
            end_date = end_date.max(date);
            let semver = &version.num;
            let downloads_by_version = match downloads_by_version_and_date.get_mut(&semver) {
                Some(by_version) => by_version,
                None => {
                    downloads_by_version_and_date.insert(semver, HashMap::new());
                    downloads_by_version_and_date.get_mut(semver).unwrap()
                }
            };
            downloads_by_version.insert(date, version_and_date.downloads);
        }
    }
    info!("Start date: {}", start_date);
    info!("End date: {}", end_date);
    let mut semvers: Vec<_> = semvers.iter().collect();
    semvers.sort_by_key(|(id, _semver)| *id);
    let semvers: Vec<_> = semvers.into_iter()
        .map(|(_id, semver)| semver)
        .filter(|semver| downloads_by_version_and_date.contains_key(&**semver)).collect();
    out.write_all(semvers.iter().map(|ver| format!("\"{}\"", ver)).collect::<Vec<_>>().join(",").as_bytes())?;
    out.write_all(&[b'\n'])?;
    let downloads_by_version: Vec<_> = semvers.into_iter()
        .flat_map(|semver| downloads_by_version_and_date.get(semver))
        .collect();
    for date in start_date.iter_days() {
        out.write_all(format!("\"{}\",", date).as_bytes())?;
        out.write_all(downloads_by_version.iter()
            .map(|semver_data| semver_data.get(&date).unwrap_or(&0).to_string())
            .collect::<Vec<_>>()
            .join(",")
            .as_bytes())?;
        out.write_all(&[b'\n'])?;
        if date >= end_date {
            return Ok(());
        }
    }
    Ok(())
}
