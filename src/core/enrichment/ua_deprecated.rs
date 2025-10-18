use rust_device_detector::device_detector::{DeviceDetector, KnownDevice};
use crate::core::enrichment::device::{DeviceInfo, DeviceType};

pub struct DeviceLookupOld {
    ua: DeviceDetector
}

#[allow(dead_code)]
impl DeviceLookupOld {

    const UA_SAMPLE: &'static str = "Dalvik/2.1.0 (Linux; U; Android 14; SM-X306B Build/UP1A.231005.007)";

    pub fn new(cache_sz: usize) -> Self {
        assert!(cache_sz > 0, "cache sz must be greater than 0");

        let lookup = DeviceDetector::new_with_cache(cache_sz as u64);

        // Important because data apparently only loads upon first lookup
        lookup.parse(Self::UA_SAMPLE, None).expect("Priming UA lookup should succeed");

        DeviceLookupOld {
            ua: lookup
        }
    }

    fn extract_devtype(known_device: &KnownDevice) -> DeviceType {
        if known_device.is_desktop()
            || known_device.is_notebook() {
            return DeviceType::Desktop;
        } else if known_device.is_feature_phone()
            || known_device.is_mobile()
            || known_device.is_mobile_app()
            || known_device.is_phablet()
            || known_device.is_smart_phone() {
            return DeviceType::Phone;
        } else if known_device.is_tablet() {
            return DeviceType::Tablet;
        } else if known_device.is_television() {
            return DeviceType::Tv;
        } else if known_device.is_console()
            || known_device.is_media_player()
            || known_device.is_smart_display() {
            return DeviceType::SetTop;
        }

        DeviceType::Unknown
    }

    pub fn lookup_device(&self, user_agent: &str) -> Option<DeviceInfo> {
        let detection_result = self.ua.parse(user_agent, None);
        println!("1. detection result {:?}", detection_result);

        if let Err(_) = detection_result {
            println!("2. parse error, returning None");
            return None;
        }

        let detection = detection_result.unwrap();
        println!("3. unwrapped detection");

        if detection.is_bot() {
            println!("4. is bot, returning Bot DeviceInfo");
            return Some(DeviceInfo {
                devtype: DeviceType::Bot,
                ..Default::default()
            });
        }

        println!("5. not a bot, getting known device");
        let known_device_opt = detection.get_known_device();
        println!("6. got known_device_opt, is_some: {}", known_device_opt.is_some());

        if known_device_opt.is_none() {
            println!("7. no known device, returning Unknown");
            return Some(DeviceInfo {
                devtype: DeviceType::Unknown,
                ..Default::default()
            });
        }

        let known_device = known_device_opt.unwrap();
        println!("8. unwrapped known_device");

        let mut brand: Option<String> = None;
        let mut model: Option<String> = None;
        let mut os: Option<String> = None;
        let devtype = Self::extract_devtype(known_device);

        if let Some(device) = &known_device.device {
            brand = device.brand.clone();
            model = device.model.clone();
        }

        if let Some(dos) = &known_device.os {
            os = Some(dos.name.clone());
        }

        println!("15. creating final DeviceInfo");
        Some(DeviceInfo {
            brand,
            model,
            os,
            devtype
        })
    }
}