use anyhow::{Error, anyhow, bail};
use rtb::bid_response::Bid;
use rtb::bid_response::bid::AdmOneof;
use rtb::utils::adm::AdFormat;
use rtb::utils::trackers::VastTrackersBuilder;

fn inject_banner_beacon(beacon_url: &String, bid: &mut Bid) -> Result<(), Error> {
    let pixel_html = rtb::utils::trackers::html_pixel(beacon_url)?;

    let adm = match &bid.adm_oneof.as_ref().unwrap() {
        AdmOneof::Adm(markup) => markup,
        _ => bail!("Banner markup should be adm not admnative!"),
    };

    let new_markup = adm.to_owned() + pixel_html.as_str();
    let new_adm_obj = AdmOneof::Adm(new_markup);

    bid.adm_oneof = Some(new_adm_obj);

    Ok(())
}

fn inject_vast_beacon(beacon_url: &String, bid: &mut Bid) -> Result<(), Error> {
    let adm = match &bid.adm_oneof.as_ref().unwrap() {
        AdmOneof::Adm(markup) => markup,
        _ => bail!("Video markup should be adm not admnative!"),
    };

    let trackers = VastTrackersBuilder::default()
        .impression(Some(beacon_url.clone()))
        .build()
        .map_err(|e| anyhow!("Failed to build VastTrackers: {}", e))?;

    let new_vast = match rtb::utils::trackers::inject_vast_trackers(adm.as_str(), &trackers) {
        Ok(new_vast) => new_vast,
        Err(e) => bail!("Failed to inject vast trackers: {}", e),
    };

    let new_adm_obj = AdmOneof::Adm(new_vast);

    bid.adm_oneof = Some(new_adm_obj);

    Ok(())
}

pub fn inject_adm_beacon(beacon_url: String, bid: &mut Bid) -> Result<(), Error> {
    let ad_format = match rtb::utils::detect_ad_format(bid) {
        Some(ad_format) => ad_format,
        None => bail!("Failed to detect adm type for bid!"),
    };

    match ad_format {
        AdFormat::Banner => inject_banner_beacon(&beacon_url, bid),
        AdFormat::Video => inject_vast_beacon(&beacon_url, bid),
        _ => bail!("Unsupported format: {:?}", ad_format),
    }
}
