use anyhow::{Error, bail};
use log::debug;
use rtb::bid_response::Bid;
use rtb::bid_response::bid::AdmOneof;
use rtb::utils::adformats::AdFormat;

fn inject_banner_beacon(beacon_url: &String, bid: &mut Bid) -> Result<(), Error> {
    let pixel_html = rtb::utils::html_pixel(beacon_url)?;

    let adm = match &bid.adm_oneof.as_ref().unwrap() {
        AdmOneof::Adm(markup) => markup,
        _ => bail!("Banner markup should be adm not admnative!"),
    };

    let new_markup = adm.to_owned() + pixel_html.as_str();
    debug!("Adm post beacon: {}", &new_markup);
    let new_adm_obj = AdmOneof::Adm(new_markup);

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
        _ => bail!("Unsupported format: {:?}", ad_format),
    }
}
