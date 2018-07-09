// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(PartialEq,Clone,Default)]
pub struct ZoneAddress {
    // message fields
    zone_id: ::protobuf::SingularField<::std::string::String>,
    address: ::protobuf::SingularField<::std::string::String>,
    swim_port: ::std::option::Option<i32>,
    gossip_port: ::std::option::Option<i32>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ZoneAddress {}

impl ZoneAddress {
    pub fn new() -> ZoneAddress {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ZoneAddress {
        static mut instance: ::protobuf::lazy::Lazy<ZoneAddress> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ZoneAddress,
        };
        unsafe {
            instance.get(ZoneAddress::new)
        }
    }

    // optional string zone_id = 1;

    pub fn clear_zone_id(&mut self) {
        self.zone_id.clear();
    }

    pub fn has_zone_id(&self) -> bool {
        self.zone_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_zone_id(&mut self, v: ::std::string::String) {
        self.zone_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_zone_id(&mut self) -> &mut ::std::string::String {
        if self.zone_id.is_none() {
            self.zone_id.set_default();
        }
        self.zone_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_zone_id(&mut self) -> ::std::string::String {
        self.zone_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_zone_id(&self) -> &str {
        match self.zone_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_zone_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.zone_id
    }

    fn mut_zone_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.zone_id
    }

    // optional string address = 2;

    pub fn clear_address(&mut self) {
        self.address.clear();
    }

    pub fn has_address(&self) -> bool {
        self.address.is_some()
    }

    // Param is passed by value, moved
    pub fn set_address(&mut self, v: ::std::string::String) {
        self.address = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_address(&mut self) -> &mut ::std::string::String {
        if self.address.is_none() {
            self.address.set_default();
        }
        self.address.as_mut().unwrap()
    }

    // Take field
    pub fn take_address(&mut self) -> ::std::string::String {
        self.address.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_address(&self) -> &str {
        match self.address.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_address_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.address
    }

    fn mut_address_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.address
    }

    // optional int32 swim_port = 3;

    pub fn clear_swim_port(&mut self) {
        self.swim_port = ::std::option::Option::None;
    }

    pub fn has_swim_port(&self) -> bool {
        self.swim_port.is_some()
    }

    // Param is passed by value, moved
    pub fn set_swim_port(&mut self, v: i32) {
        self.swim_port = ::std::option::Option::Some(v);
    }

    pub fn get_swim_port(&self) -> i32 {
        self.swim_port.unwrap_or(0)
    }

    fn get_swim_port_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.swim_port
    }

    fn mut_swim_port_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.swim_port
    }

    // optional int32 gossip_port = 4;

    pub fn clear_gossip_port(&mut self) {
        self.gossip_port = ::std::option::Option::None;
    }

    pub fn has_gossip_port(&self) -> bool {
        self.gossip_port.is_some()
    }

    // Param is passed by value, moved
    pub fn set_gossip_port(&mut self, v: i32) {
        self.gossip_port = ::std::option::Option::Some(v);
    }

    pub fn get_gossip_port(&self) -> i32 {
        self.gossip_port.unwrap_or(0)
    }

    fn get_gossip_port_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.gossip_port
    }

    fn mut_gossip_port_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.gossip_port
    }
}

impl ::protobuf::Message for ZoneAddress {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.zone_id)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.address)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.swim_port = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.gossip_port = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.zone_id.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(ref v) = self.address.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        if let Some(v) = self.swim_port {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.gossip_port {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.zone_id.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(ref v) = self.address.as_ref() {
            os.write_string(2, &v)?;
        }
        if let Some(v) = self.swim_port {
            os.write_int32(3, v)?;
        }
        if let Some(v) = self.gossip_port {
            os.write_int32(4, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ZoneAddress {
    fn new() -> ZoneAddress {
        ZoneAddress::new()
    }

    fn descriptor_static(_: ::std::option::Option<ZoneAddress>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "zone_id",
                    ZoneAddress::get_zone_id_for_reflect,
                    ZoneAddress::mut_zone_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "address",
                    ZoneAddress::get_address_for_reflect,
                    ZoneAddress::mut_address_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "swim_port",
                    ZoneAddress::get_swim_port_for_reflect,
                    ZoneAddress::mut_swim_port_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "gossip_port",
                    ZoneAddress::get_gossip_port_for_reflect,
                    ZoneAddress::mut_gossip_port_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ZoneAddress>(
                    "ZoneAddress",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ZoneAddress {
    fn clear(&mut self) {
        self.clear_zone_id();
        self.clear_address();
        self.clear_swim_port();
        self.clear_gossip_port();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ZoneAddress {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ZoneAddress {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Member {
    // message fields
    id: ::protobuf::SingularField<::std::string::String>,
    incarnation: ::std::option::Option<u64>,
    address: ::protobuf::SingularField<::std::string::String>,
    swim_port: ::std::option::Option<i32>,
    gossip_port: ::std::option::Option<i32>,
    persistent: ::std::option::Option<bool>,
    departed: ::std::option::Option<bool>,
    zone_id: ::protobuf::SingularField<::std::string::String>,
    additional_addresses: ::protobuf::RepeatedField<ZoneAddress>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Member {}

impl Member {
    pub fn new() -> Member {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Member {
        static mut instance: ::protobuf::lazy::Lazy<Member> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Member,
        };
        unsafe {
            instance.get(Member::new)
        }
    }

    // optional string id = 1;

    pub fn clear_id(&mut self) {
        self.id.clear();
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: ::std::string::String) {
        self.id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_id(&mut self) -> &mut ::std::string::String {
        if self.id.is_none() {
            self.id.set_default();
        }
        self.id.as_mut().unwrap()
    }

    // Take field
    pub fn take_id(&mut self) -> ::std::string::String {
        self.id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_id(&self) -> &str {
        match self.id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.id
    }

    fn mut_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.id
    }

    // optional uint64 incarnation = 2;

    pub fn clear_incarnation(&mut self) {
        self.incarnation = ::std::option::Option::None;
    }

    pub fn has_incarnation(&self) -> bool {
        self.incarnation.is_some()
    }

    // Param is passed by value, moved
    pub fn set_incarnation(&mut self, v: u64) {
        self.incarnation = ::std::option::Option::Some(v);
    }

    pub fn get_incarnation(&self) -> u64 {
        self.incarnation.unwrap_or(0)
    }

    fn get_incarnation_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.incarnation
    }

    fn mut_incarnation_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.incarnation
    }

    // optional string address = 3;

    pub fn clear_address(&mut self) {
        self.address.clear();
    }

    pub fn has_address(&self) -> bool {
        self.address.is_some()
    }

    // Param is passed by value, moved
    pub fn set_address(&mut self, v: ::std::string::String) {
        self.address = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_address(&mut self) -> &mut ::std::string::String {
        if self.address.is_none() {
            self.address.set_default();
        }
        self.address.as_mut().unwrap()
    }

    // Take field
    pub fn take_address(&mut self) -> ::std::string::String {
        self.address.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_address(&self) -> &str {
        match self.address.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_address_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.address
    }

    fn mut_address_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.address
    }

    // optional int32 swim_port = 4;

    pub fn clear_swim_port(&mut self) {
        self.swim_port = ::std::option::Option::None;
    }

    pub fn has_swim_port(&self) -> bool {
        self.swim_port.is_some()
    }

    // Param is passed by value, moved
    pub fn set_swim_port(&mut self, v: i32) {
        self.swim_port = ::std::option::Option::Some(v);
    }

    pub fn get_swim_port(&self) -> i32 {
        self.swim_port.unwrap_or(0)
    }

    fn get_swim_port_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.swim_port
    }

    fn mut_swim_port_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.swim_port
    }

    // optional int32 gossip_port = 5;

    pub fn clear_gossip_port(&mut self) {
        self.gossip_port = ::std::option::Option::None;
    }

    pub fn has_gossip_port(&self) -> bool {
        self.gossip_port.is_some()
    }

    // Param is passed by value, moved
    pub fn set_gossip_port(&mut self, v: i32) {
        self.gossip_port = ::std::option::Option::Some(v);
    }

    pub fn get_gossip_port(&self) -> i32 {
        self.gossip_port.unwrap_or(0)
    }

    fn get_gossip_port_for_reflect(&self) -> &::std::option::Option<i32> {
        &self.gossip_port
    }

    fn mut_gossip_port_for_reflect(&mut self) -> &mut ::std::option::Option<i32> {
        &mut self.gossip_port
    }

    // optional bool persistent = 6;

    pub fn clear_persistent(&mut self) {
        self.persistent = ::std::option::Option::None;
    }

    pub fn has_persistent(&self) -> bool {
        self.persistent.is_some()
    }

    // Param is passed by value, moved
    pub fn set_persistent(&mut self, v: bool) {
        self.persistent = ::std::option::Option::Some(v);
    }

    pub fn get_persistent(&self) -> bool {
        self.persistent.unwrap_or(false)
    }

    fn get_persistent_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.persistent
    }

    fn mut_persistent_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.persistent
    }

    // optional bool departed = 7;

    pub fn clear_departed(&mut self) {
        self.departed = ::std::option::Option::None;
    }

    pub fn has_departed(&self) -> bool {
        self.departed.is_some()
    }

    // Param is passed by value, moved
    pub fn set_departed(&mut self, v: bool) {
        self.departed = ::std::option::Option::Some(v);
    }

    pub fn get_departed(&self) -> bool {
        self.departed.unwrap_or(false)
    }

    fn get_departed_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.departed
    }

    fn mut_departed_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.departed
    }

    // optional string zone_id = 8;

    pub fn clear_zone_id(&mut self) {
        self.zone_id.clear();
    }

    pub fn has_zone_id(&self) -> bool {
        self.zone_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_zone_id(&mut self, v: ::std::string::String) {
        self.zone_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_zone_id(&mut self) -> &mut ::std::string::String {
        if self.zone_id.is_none() {
            self.zone_id.set_default();
        }
        self.zone_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_zone_id(&mut self) -> ::std::string::String {
        self.zone_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_zone_id(&self) -> &str {
        match self.zone_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_zone_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.zone_id
    }

    fn mut_zone_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.zone_id
    }

    // repeated .ZoneAddress additional_addresses = 9;

    pub fn clear_additional_addresses(&mut self) {
        self.additional_addresses.clear();
    }

    // Param is passed by value, moved
    pub fn set_additional_addresses(&mut self, v: ::protobuf::RepeatedField<ZoneAddress>) {
        self.additional_addresses = v;
    }

    // Mutable pointer to the field.
    pub fn mut_additional_addresses(&mut self) -> &mut ::protobuf::RepeatedField<ZoneAddress> {
        &mut self.additional_addresses
    }

    // Take field
    pub fn take_additional_addresses(&mut self) -> ::protobuf::RepeatedField<ZoneAddress> {
        ::std::mem::replace(&mut self.additional_addresses, ::protobuf::RepeatedField::new())
    }

    pub fn get_additional_addresses(&self) -> &[ZoneAddress] {
        &self.additional_addresses
    }

    fn get_additional_addresses_for_reflect(&self) -> &::protobuf::RepeatedField<ZoneAddress> {
        &self.additional_addresses
    }

    fn mut_additional_addresses_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<ZoneAddress> {
        &mut self.additional_addresses
    }
}

impl ::protobuf::Message for Member {
    fn is_initialized(&self) -> bool {
        for v in &self.additional_addresses {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.id)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.incarnation = ::std::option::Option::Some(tmp);
                },
                3 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.address)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.swim_port = ::std::option::Option::Some(tmp);
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_int32()?;
                    self.gossip_port = ::std::option::Option::Some(tmp);
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.persistent = ::std::option::Option::Some(tmp);
                },
                7 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.departed = ::std::option::Option::Some(tmp);
                },
                8 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.zone_id)?;
                },
                9 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.additional_addresses)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.id.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(v) = self.incarnation {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.address.as_ref() {
            my_size += ::protobuf::rt::string_size(3, &v);
        }
        if let Some(v) = self.swim_port {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.gossip_port {
            my_size += ::protobuf::rt::value_size(5, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.persistent {
            my_size += 2;
        }
        if let Some(v) = self.departed {
            my_size += 2;
        }
        if let Some(ref v) = self.zone_id.as_ref() {
            my_size += ::protobuf::rt::string_size(8, &v);
        }
        for value in &self.additional_addresses {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.id.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(v) = self.incarnation {
            os.write_uint64(2, v)?;
        }
        if let Some(ref v) = self.address.as_ref() {
            os.write_string(3, &v)?;
        }
        if let Some(v) = self.swim_port {
            os.write_int32(4, v)?;
        }
        if let Some(v) = self.gossip_port {
            os.write_int32(5, v)?;
        }
        if let Some(v) = self.persistent {
            os.write_bool(6, v)?;
        }
        if let Some(v) = self.departed {
            os.write_bool(7, v)?;
        }
        if let Some(ref v) = self.zone_id.as_ref() {
            os.write_string(8, &v)?;
        }
        for v in &self.additional_addresses {
            os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Member {
    fn new() -> Member {
        Member::new()
    }

    fn descriptor_static(_: ::std::option::Option<Member>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "id",
                    Member::get_id_for_reflect,
                    Member::mut_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "incarnation",
                    Member::get_incarnation_for_reflect,
                    Member::mut_incarnation_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "address",
                    Member::get_address_for_reflect,
                    Member::mut_address_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "swim_port",
                    Member::get_swim_port_for_reflect,
                    Member::mut_swim_port_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeInt32>(
                    "gossip_port",
                    Member::get_gossip_port_for_reflect,
                    Member::mut_gossip_port_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "persistent",
                    Member::get_persistent_for_reflect,
                    Member::mut_persistent_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "departed",
                    Member::get_departed_for_reflect,
                    Member::mut_departed_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "zone_id",
                    Member::get_zone_id_for_reflect,
                    Member::mut_zone_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ZoneAddress>>(
                    "additional_addresses",
                    Member::get_additional_addresses_for_reflect,
                    Member::mut_additional_addresses_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Member>(
                    "Member",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Member {
    fn clear(&mut self) {
        self.clear_id();
        self.clear_incarnation();
        self.clear_address();
        self.clear_swim_port();
        self.clear_gossip_port();
        self.clear_persistent();
        self.clear_departed();
        self.clear_zone_id();
        self.clear_additional_addresses();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Member {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Member {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Zone {
    // message fields
    id: ::protobuf::SingularField<::std::string::String>,
    incarnation: ::std::option::Option<u64>,
    maintainer_id: ::protobuf::SingularField<::std::string::String>,
    parent_zone_id: ::protobuf::SingularField<::std::string::String>,
    child_zone_ids: ::protobuf::RepeatedField<::std::string::String>,
    successor: ::protobuf::SingularField<::std::string::String>,
    predecessors: ::protobuf::RepeatedField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Zone {}

impl Zone {
    pub fn new() -> Zone {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Zone {
        static mut instance: ::protobuf::lazy::Lazy<Zone> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Zone,
        };
        unsafe {
            instance.get(Zone::new)
        }
    }

    // optional string id = 1;

    pub fn clear_id(&mut self) {
        self.id.clear();
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: ::std::string::String) {
        self.id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_id(&mut self) -> &mut ::std::string::String {
        if self.id.is_none() {
            self.id.set_default();
        }
        self.id.as_mut().unwrap()
    }

    // Take field
    pub fn take_id(&mut self) -> ::std::string::String {
        self.id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_id(&self) -> &str {
        match self.id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.id
    }

    fn mut_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.id
    }

    // optional uint64 incarnation = 2;

    pub fn clear_incarnation(&mut self) {
        self.incarnation = ::std::option::Option::None;
    }

    pub fn has_incarnation(&self) -> bool {
        self.incarnation.is_some()
    }

    // Param is passed by value, moved
    pub fn set_incarnation(&mut self, v: u64) {
        self.incarnation = ::std::option::Option::Some(v);
    }

    pub fn get_incarnation(&self) -> u64 {
        self.incarnation.unwrap_or(0)
    }

    fn get_incarnation_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.incarnation
    }

    fn mut_incarnation_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.incarnation
    }

    // optional string maintainer_id = 3;

    pub fn clear_maintainer_id(&mut self) {
        self.maintainer_id.clear();
    }

    pub fn has_maintainer_id(&self) -> bool {
        self.maintainer_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_maintainer_id(&mut self, v: ::std::string::String) {
        self.maintainer_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_maintainer_id(&mut self) -> &mut ::std::string::String {
        if self.maintainer_id.is_none() {
            self.maintainer_id.set_default();
        }
        self.maintainer_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_maintainer_id(&mut self) -> ::std::string::String {
        self.maintainer_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_maintainer_id(&self) -> &str {
        match self.maintainer_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_maintainer_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.maintainer_id
    }

    fn mut_maintainer_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.maintainer_id
    }

    // optional string parent_zone_id = 4;

    pub fn clear_parent_zone_id(&mut self) {
        self.parent_zone_id.clear();
    }

    pub fn has_parent_zone_id(&self) -> bool {
        self.parent_zone_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_parent_zone_id(&mut self, v: ::std::string::String) {
        self.parent_zone_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_parent_zone_id(&mut self) -> &mut ::std::string::String {
        if self.parent_zone_id.is_none() {
            self.parent_zone_id.set_default();
        }
        self.parent_zone_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_parent_zone_id(&mut self) -> ::std::string::String {
        self.parent_zone_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_parent_zone_id(&self) -> &str {
        match self.parent_zone_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_parent_zone_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.parent_zone_id
    }

    fn mut_parent_zone_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.parent_zone_id
    }

    // repeated string child_zone_ids = 5;

    pub fn clear_child_zone_ids(&mut self) {
        self.child_zone_ids.clear();
    }

    // Param is passed by value, moved
    pub fn set_child_zone_ids(&mut self, v: ::protobuf::RepeatedField<::std::string::String>) {
        self.child_zone_ids = v;
    }

    // Mutable pointer to the field.
    pub fn mut_child_zone_ids(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.child_zone_ids
    }

    // Take field
    pub fn take_child_zone_ids(&mut self) -> ::protobuf::RepeatedField<::std::string::String> {
        ::std::mem::replace(&mut self.child_zone_ids, ::protobuf::RepeatedField::new())
    }

    pub fn get_child_zone_ids(&self) -> &[::std::string::String] {
        &self.child_zone_ids
    }

    fn get_child_zone_ids_for_reflect(&self) -> &::protobuf::RepeatedField<::std::string::String> {
        &self.child_zone_ids
    }

    fn mut_child_zone_ids_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.child_zone_ids
    }

    // optional string successor = 6;

    pub fn clear_successor(&mut self) {
        self.successor.clear();
    }

    pub fn has_successor(&self) -> bool {
        self.successor.is_some()
    }

    // Param is passed by value, moved
    pub fn set_successor(&mut self, v: ::std::string::String) {
        self.successor = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_successor(&mut self) -> &mut ::std::string::String {
        if self.successor.is_none() {
            self.successor.set_default();
        }
        self.successor.as_mut().unwrap()
    }

    // Take field
    pub fn take_successor(&mut self) -> ::std::string::String {
        self.successor.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_successor(&self) -> &str {
        match self.successor.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_successor_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.successor
    }

    fn mut_successor_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.successor
    }

    // repeated string predecessors = 7;

    pub fn clear_predecessors(&mut self) {
        self.predecessors.clear();
    }

    // Param is passed by value, moved
    pub fn set_predecessors(&mut self, v: ::protobuf::RepeatedField<::std::string::String>) {
        self.predecessors = v;
    }

    // Mutable pointer to the field.
    pub fn mut_predecessors(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.predecessors
    }

    // Take field
    pub fn take_predecessors(&mut self) -> ::protobuf::RepeatedField<::std::string::String> {
        ::std::mem::replace(&mut self.predecessors, ::protobuf::RepeatedField::new())
    }

    pub fn get_predecessors(&self) -> &[::std::string::String] {
        &self.predecessors
    }

    fn get_predecessors_for_reflect(&self) -> &::protobuf::RepeatedField<::std::string::String> {
        &self.predecessors
    }

    fn mut_predecessors_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.predecessors
    }
}

impl ::protobuf::Message for Zone {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.id)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.incarnation = ::std::option::Option::Some(tmp);
                },
                3 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.maintainer_id)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.parent_zone_id)?;
                },
                5 => {
                    ::protobuf::rt::read_repeated_string_into(wire_type, is, &mut self.child_zone_ids)?;
                },
                6 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.successor)?;
                },
                7 => {
                    ::protobuf::rt::read_repeated_string_into(wire_type, is, &mut self.predecessors)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.id.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(v) = self.incarnation {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.maintainer_id.as_ref() {
            my_size += ::protobuf::rt::string_size(3, &v);
        }
        if let Some(ref v) = self.parent_zone_id.as_ref() {
            my_size += ::protobuf::rt::string_size(4, &v);
        }
        for value in &self.child_zone_ids {
            my_size += ::protobuf::rt::string_size(5, &value);
        };
        if let Some(ref v) = self.successor.as_ref() {
            my_size += ::protobuf::rt::string_size(6, &v);
        }
        for value in &self.predecessors {
            my_size += ::protobuf::rt::string_size(7, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.id.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(v) = self.incarnation {
            os.write_uint64(2, v)?;
        }
        if let Some(ref v) = self.maintainer_id.as_ref() {
            os.write_string(3, &v)?;
        }
        if let Some(ref v) = self.parent_zone_id.as_ref() {
            os.write_string(4, &v)?;
        }
        for v in &self.child_zone_ids {
            os.write_string(5, &v)?;
        };
        if let Some(ref v) = self.successor.as_ref() {
            os.write_string(6, &v)?;
        }
        for v in &self.predecessors {
            os.write_string(7, &v)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Zone {
    fn new() -> Zone {
        Zone::new()
    }

    fn descriptor_static(_: ::std::option::Option<Zone>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "id",
                    Zone::get_id_for_reflect,
                    Zone::mut_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "incarnation",
                    Zone::get_incarnation_for_reflect,
                    Zone::mut_incarnation_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "maintainer_id",
                    Zone::get_maintainer_id_for_reflect,
                    Zone::mut_maintainer_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "parent_zone_id",
                    Zone::get_parent_zone_id_for_reflect,
                    Zone::mut_parent_zone_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "child_zone_ids",
                    Zone::get_child_zone_ids_for_reflect,
                    Zone::mut_child_zone_ids_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "successor",
                    Zone::get_successor_for_reflect,
                    Zone::mut_successor_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "predecessors",
                    Zone::get_predecessors_for_reflect,
                    Zone::mut_predecessors_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Zone>(
                    "Zone",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Zone {
    fn clear(&mut self) {
        self.clear_id();
        self.clear_incarnation();
        self.clear_maintainer_id();
        self.clear_parent_zone_id();
        self.clear_child_zone_ids();
        self.clear_successor();
        self.clear_predecessors();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Zone {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Zone {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Ping {
    // message fields
    from: ::protobuf::SingularPtrField<Member>,
    forward_to: ::protobuf::SingularPtrField<Member>,
    to: ::protobuf::SingularPtrField<Member>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Ping {}

impl Ping {
    pub fn new() -> Ping {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Ping {
        static mut instance: ::protobuf::lazy::Lazy<Ping> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Ping,
        };
        unsafe {
            instance.get(Ping::new)
        }
    }

    // optional .Member from = 1;

    pub fn clear_from(&mut self) {
        self.from.clear();
    }

    pub fn has_from(&self) -> bool {
        self.from.is_some()
    }

    // Param is passed by value, moved
    pub fn set_from(&mut self, v: Member) {
        self.from = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_from(&mut self) -> &mut Member {
        if self.from.is_none() {
            self.from.set_default();
        }
        self.from.as_mut().unwrap()
    }

    // Take field
    pub fn take_from(&mut self) -> Member {
        self.from.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_from(&self) -> &Member {
        self.from.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_from_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.from
    }

    fn mut_from_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.from
    }

    // optional .Member forward_to = 2;

    pub fn clear_forward_to(&mut self) {
        self.forward_to.clear();
    }

    pub fn has_forward_to(&self) -> bool {
        self.forward_to.is_some()
    }

    // Param is passed by value, moved
    pub fn set_forward_to(&mut self, v: Member) {
        self.forward_to = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_forward_to(&mut self) -> &mut Member {
        if self.forward_to.is_none() {
            self.forward_to.set_default();
        }
        self.forward_to.as_mut().unwrap()
    }

    // Take field
    pub fn take_forward_to(&mut self) -> Member {
        self.forward_to.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_forward_to(&self) -> &Member {
        self.forward_to.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_forward_to_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.forward_to
    }

    fn mut_forward_to_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.forward_to
    }

    // optional .Member to = 3;

    pub fn clear_to(&mut self) {
        self.to.clear();
    }

    pub fn has_to(&self) -> bool {
        self.to.is_some()
    }

    // Param is passed by value, moved
    pub fn set_to(&mut self, v: Member) {
        self.to = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_to(&mut self) -> &mut Member {
        if self.to.is_none() {
            self.to.set_default();
        }
        self.to.as_mut().unwrap()
    }

    // Take field
    pub fn take_to(&mut self) -> Member {
        self.to.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_to(&self) -> &Member {
        self.to.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_to_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.to
    }

    fn mut_to_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.to
    }
}

impl ::protobuf::Message for Ping {
    fn is_initialized(&self) -> bool {
        for v in &self.from {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.forward_to {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.to {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.from)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.forward_to)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.to)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.from.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.forward_to.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.to.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.from.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.forward_to.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.to.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Ping {
    fn new() -> Ping {
        Ping::new()
    }

    fn descriptor_static(_: ::std::option::Option<Ping>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "from",
                    Ping::get_from_for_reflect,
                    Ping::mut_from_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "forward_to",
                    Ping::get_forward_to_for_reflect,
                    Ping::mut_forward_to_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "to",
                    Ping::get_to_for_reflect,
                    Ping::mut_to_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Ping>(
                    "Ping",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Ping {
    fn clear(&mut self) {
        self.clear_from();
        self.clear_forward_to();
        self.clear_to();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Ping {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Ping {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Ack {
    // message fields
    from: ::protobuf::SingularPtrField<Member>,
    forward_to: ::protobuf::SingularPtrField<Member>,
    to: ::protobuf::SingularPtrField<Member>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Ack {}

impl Ack {
    pub fn new() -> Ack {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Ack {
        static mut instance: ::protobuf::lazy::Lazy<Ack> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Ack,
        };
        unsafe {
            instance.get(Ack::new)
        }
    }

    // optional .Member from = 1;

    pub fn clear_from(&mut self) {
        self.from.clear();
    }

    pub fn has_from(&self) -> bool {
        self.from.is_some()
    }

    // Param is passed by value, moved
    pub fn set_from(&mut self, v: Member) {
        self.from = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_from(&mut self) -> &mut Member {
        if self.from.is_none() {
            self.from.set_default();
        }
        self.from.as_mut().unwrap()
    }

    // Take field
    pub fn take_from(&mut self) -> Member {
        self.from.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_from(&self) -> &Member {
        self.from.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_from_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.from
    }

    fn mut_from_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.from
    }

    // optional .Member forward_to = 2;

    pub fn clear_forward_to(&mut self) {
        self.forward_to.clear();
    }

    pub fn has_forward_to(&self) -> bool {
        self.forward_to.is_some()
    }

    // Param is passed by value, moved
    pub fn set_forward_to(&mut self, v: Member) {
        self.forward_to = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_forward_to(&mut self) -> &mut Member {
        if self.forward_to.is_none() {
            self.forward_to.set_default();
        }
        self.forward_to.as_mut().unwrap()
    }

    // Take field
    pub fn take_forward_to(&mut self) -> Member {
        self.forward_to.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_forward_to(&self) -> &Member {
        self.forward_to.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_forward_to_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.forward_to
    }

    fn mut_forward_to_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.forward_to
    }

    // optional .Member to = 3;

    pub fn clear_to(&mut self) {
        self.to.clear();
    }

    pub fn has_to(&self) -> bool {
        self.to.is_some()
    }

    // Param is passed by value, moved
    pub fn set_to(&mut self, v: Member) {
        self.to = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_to(&mut self) -> &mut Member {
        if self.to.is_none() {
            self.to.set_default();
        }
        self.to.as_mut().unwrap()
    }

    // Take field
    pub fn take_to(&mut self) -> Member {
        self.to.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_to(&self) -> &Member {
        self.to.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_to_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.to
    }

    fn mut_to_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.to
    }
}

impl ::protobuf::Message for Ack {
    fn is_initialized(&self) -> bool {
        for v in &self.from {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.forward_to {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.to {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.from)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.forward_to)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.to)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.from.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.forward_to.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.to.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.from.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.forward_to.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.to.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Ack {
    fn new() -> Ack {
        Ack::new()
    }

    fn descriptor_static(_: ::std::option::Option<Ack>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "from",
                    Ack::get_from_for_reflect,
                    Ack::mut_from_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "forward_to",
                    Ack::get_forward_to_for_reflect,
                    Ack::mut_forward_to_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "to",
                    Ack::get_to_for_reflect,
                    Ack::mut_to_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Ack>(
                    "Ack",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Ack {
    fn clear(&mut self) {
        self.clear_from();
        self.clear_forward_to();
        self.clear_to();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Ack {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Ack {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct PingReq {
    // message fields
    from: ::protobuf::SingularPtrField<Member>,
    target: ::protobuf::SingularPtrField<Member>,
    to: ::protobuf::SingularPtrField<Member>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PingReq {}

impl PingReq {
    pub fn new() -> PingReq {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PingReq {
        static mut instance: ::protobuf::lazy::Lazy<PingReq> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PingReq,
        };
        unsafe {
            instance.get(PingReq::new)
        }
    }

    // optional .Member from = 1;

    pub fn clear_from(&mut self) {
        self.from.clear();
    }

    pub fn has_from(&self) -> bool {
        self.from.is_some()
    }

    // Param is passed by value, moved
    pub fn set_from(&mut self, v: Member) {
        self.from = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_from(&mut self) -> &mut Member {
        if self.from.is_none() {
            self.from.set_default();
        }
        self.from.as_mut().unwrap()
    }

    // Take field
    pub fn take_from(&mut self) -> Member {
        self.from.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_from(&self) -> &Member {
        self.from.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_from_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.from
    }

    fn mut_from_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.from
    }

    // optional .Member target = 2;

    pub fn clear_target(&mut self) {
        self.target.clear();
    }

    pub fn has_target(&self) -> bool {
        self.target.is_some()
    }

    // Param is passed by value, moved
    pub fn set_target(&mut self, v: Member) {
        self.target = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_target(&mut self) -> &mut Member {
        if self.target.is_none() {
            self.target.set_default();
        }
        self.target.as_mut().unwrap()
    }

    // Take field
    pub fn take_target(&mut self) -> Member {
        self.target.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_target(&self) -> &Member {
        self.target.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_target_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.target
    }

    fn mut_target_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.target
    }

    // optional .Member to = 3;

    pub fn clear_to(&mut self) {
        self.to.clear();
    }

    pub fn has_to(&self) -> bool {
        self.to.is_some()
    }

    // Param is passed by value, moved
    pub fn set_to(&mut self, v: Member) {
        self.to = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_to(&mut self) -> &mut Member {
        if self.to.is_none() {
            self.to.set_default();
        }
        self.to.as_mut().unwrap()
    }

    // Take field
    pub fn take_to(&mut self) -> Member {
        self.to.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_to(&self) -> &Member {
        self.to.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_to_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.to
    }

    fn mut_to_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.to
    }
}

impl ::protobuf::Message for PingReq {
    fn is_initialized(&self) -> bool {
        for v in &self.from {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.target {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.to {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.from)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.target)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.to)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.from.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.target.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.to.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.from.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.target.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.to.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for PingReq {
    fn new() -> PingReq {
        PingReq::new()
    }

    fn descriptor_static(_: ::std::option::Option<PingReq>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "from",
                    PingReq::get_from_for_reflect,
                    PingReq::mut_from_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "target",
                    PingReq::get_target_for_reflect,
                    PingReq::mut_target_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "to",
                    PingReq::get_to_for_reflect,
                    PingReq::mut_to_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PingReq>(
                    "PingReq",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PingReq {
    fn clear(&mut self) {
        self.clear_from();
        self.clear_target();
        self.clear_to();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for PingReq {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for PingReq {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ZoneChange {
    // message fields
    from: ::protobuf::SingularPtrField<Member>,
    zone_id: ::protobuf::SingularField<::std::string::String>,
    new_successor: ::protobuf::SingularPtrField<Zone>,
    new_predecessors: ::protobuf::RepeatedField<Zone>,
    drop_predecessors: ::protobuf::RepeatedField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ZoneChange {}

impl ZoneChange {
    pub fn new() -> ZoneChange {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ZoneChange {
        static mut instance: ::protobuf::lazy::Lazy<ZoneChange> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ZoneChange,
        };
        unsafe {
            instance.get(ZoneChange::new)
        }
    }

    // optional .Member from = 1;

    pub fn clear_from(&mut self) {
        self.from.clear();
    }

    pub fn has_from(&self) -> bool {
        self.from.is_some()
    }

    // Param is passed by value, moved
    pub fn set_from(&mut self, v: Member) {
        self.from = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_from(&mut self) -> &mut Member {
        if self.from.is_none() {
            self.from.set_default();
        }
        self.from.as_mut().unwrap()
    }

    // Take field
    pub fn take_from(&mut self) -> Member {
        self.from.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_from(&self) -> &Member {
        self.from.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_from_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.from
    }

    fn mut_from_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.from
    }

    // optional string zone_id = 2;

    pub fn clear_zone_id(&mut self) {
        self.zone_id.clear();
    }

    pub fn has_zone_id(&self) -> bool {
        self.zone_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_zone_id(&mut self, v: ::std::string::String) {
        self.zone_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_zone_id(&mut self) -> &mut ::std::string::String {
        if self.zone_id.is_none() {
            self.zone_id.set_default();
        }
        self.zone_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_zone_id(&mut self) -> ::std::string::String {
        self.zone_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_zone_id(&self) -> &str {
        match self.zone_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_zone_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.zone_id
    }

    fn mut_zone_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.zone_id
    }

    // optional .Zone new_successor = 3;

    pub fn clear_new_successor(&mut self) {
        self.new_successor.clear();
    }

    pub fn has_new_successor(&self) -> bool {
        self.new_successor.is_some()
    }

    // Param is passed by value, moved
    pub fn set_new_successor(&mut self, v: Zone) {
        self.new_successor = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_new_successor(&mut self) -> &mut Zone {
        if self.new_successor.is_none() {
            self.new_successor.set_default();
        }
        self.new_successor.as_mut().unwrap()
    }

    // Take field
    pub fn take_new_successor(&mut self) -> Zone {
        self.new_successor.take().unwrap_or_else(|| Zone::new())
    }

    pub fn get_new_successor(&self) -> &Zone {
        self.new_successor.as_ref().unwrap_or_else(|| Zone::default_instance())
    }

    fn get_new_successor_for_reflect(&self) -> &::protobuf::SingularPtrField<Zone> {
        &self.new_successor
    }

    fn mut_new_successor_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Zone> {
        &mut self.new_successor
    }

    // repeated .Zone new_predecessors = 4;

    pub fn clear_new_predecessors(&mut self) {
        self.new_predecessors.clear();
    }

    // Param is passed by value, moved
    pub fn set_new_predecessors(&mut self, v: ::protobuf::RepeatedField<Zone>) {
        self.new_predecessors = v;
    }

    // Mutable pointer to the field.
    pub fn mut_new_predecessors(&mut self) -> &mut ::protobuf::RepeatedField<Zone> {
        &mut self.new_predecessors
    }

    // Take field
    pub fn take_new_predecessors(&mut self) -> ::protobuf::RepeatedField<Zone> {
        ::std::mem::replace(&mut self.new_predecessors, ::protobuf::RepeatedField::new())
    }

    pub fn get_new_predecessors(&self) -> &[Zone] {
        &self.new_predecessors
    }

    fn get_new_predecessors_for_reflect(&self) -> &::protobuf::RepeatedField<Zone> {
        &self.new_predecessors
    }

    fn mut_new_predecessors_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Zone> {
        &mut self.new_predecessors
    }

    // repeated string drop_predecessors = 5;

    pub fn clear_drop_predecessors(&mut self) {
        self.drop_predecessors.clear();
    }

    // Param is passed by value, moved
    pub fn set_drop_predecessors(&mut self, v: ::protobuf::RepeatedField<::std::string::String>) {
        self.drop_predecessors = v;
    }

    // Mutable pointer to the field.
    pub fn mut_drop_predecessors(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.drop_predecessors
    }

    // Take field
    pub fn take_drop_predecessors(&mut self) -> ::protobuf::RepeatedField<::std::string::String> {
        ::std::mem::replace(&mut self.drop_predecessors, ::protobuf::RepeatedField::new())
    }

    pub fn get_drop_predecessors(&self) -> &[::std::string::String] {
        &self.drop_predecessors
    }

    fn get_drop_predecessors_for_reflect(&self) -> &::protobuf::RepeatedField<::std::string::String> {
        &self.drop_predecessors
    }

    fn mut_drop_predecessors_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.drop_predecessors
    }
}

impl ::protobuf::Message for ZoneChange {
    fn is_initialized(&self) -> bool {
        for v in &self.from {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.new_successor {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.new_predecessors {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.from)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.zone_id)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.new_successor)?;
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.new_predecessors)?;
                },
                5 => {
                    ::protobuf::rt::read_repeated_string_into(wire_type, is, &mut self.drop_predecessors)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.from.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.zone_id.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        if let Some(ref v) = self.new_successor.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        for value in &self.new_predecessors {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.drop_predecessors {
            my_size += ::protobuf::rt::string_size(5, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.from.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.zone_id.as_ref() {
            os.write_string(2, &v)?;
        }
        if let Some(ref v) = self.new_successor.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        for v in &self.new_predecessors {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.drop_predecessors {
            os.write_string(5, &v)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ZoneChange {
    fn new() -> ZoneChange {
        ZoneChange::new()
    }

    fn descriptor_static(_: ::std::option::Option<ZoneChange>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "from",
                    ZoneChange::get_from_for_reflect,
                    ZoneChange::mut_from_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "zone_id",
                    ZoneChange::get_zone_id_for_reflect,
                    ZoneChange::mut_zone_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Zone>>(
                    "new_successor",
                    ZoneChange::get_new_successor_for_reflect,
                    ZoneChange::mut_new_successor_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Zone>>(
                    "new_predecessors",
                    ZoneChange::get_new_predecessors_for_reflect,
                    ZoneChange::mut_new_predecessors_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "drop_predecessors",
                    ZoneChange::get_drop_predecessors_for_reflect,
                    ZoneChange::mut_drop_predecessors_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ZoneChange>(
                    "ZoneChange",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ZoneChange {
    fn clear(&mut self) {
        self.clear_from();
        self.clear_zone_id();
        self.clear_new_successor();
        self.clear_new_predecessors();
        self.clear_drop_predecessors();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ZoneChange {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ZoneChange {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Membership {
    // message fields
    member: ::protobuf::SingularPtrField<Member>,
    health: ::std::option::Option<Membership_Health>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Membership {}

impl Membership {
    pub fn new() -> Membership {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Membership {
        static mut instance: ::protobuf::lazy::Lazy<Membership> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Membership,
        };
        unsafe {
            instance.get(Membership::new)
        }
    }

    // optional .Member member = 1;

    pub fn clear_member(&mut self) {
        self.member.clear();
    }

    pub fn has_member(&self) -> bool {
        self.member.is_some()
    }

    // Param is passed by value, moved
    pub fn set_member(&mut self, v: Member) {
        self.member = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_member(&mut self) -> &mut Member {
        if self.member.is_none() {
            self.member.set_default();
        }
        self.member.as_mut().unwrap()
    }

    // Take field
    pub fn take_member(&mut self) -> Member {
        self.member.take().unwrap_or_else(|| Member::new())
    }

    pub fn get_member(&self) -> &Member {
        self.member.as_ref().unwrap_or_else(|| Member::default_instance())
    }

    fn get_member_for_reflect(&self) -> &::protobuf::SingularPtrField<Member> {
        &self.member
    }

    fn mut_member_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<Member> {
        &mut self.member
    }

    // optional .Membership.Health health = 2;

    pub fn clear_health(&mut self) {
        self.health = ::std::option::Option::None;
    }

    pub fn has_health(&self) -> bool {
        self.health.is_some()
    }

    // Param is passed by value, moved
    pub fn set_health(&mut self, v: Membership_Health) {
        self.health = ::std::option::Option::Some(v);
    }

    pub fn get_health(&self) -> Membership_Health {
        self.health.unwrap_or(Membership_Health::ALIVE)
    }

    fn get_health_for_reflect(&self) -> &::std::option::Option<Membership_Health> {
        &self.health
    }

    fn mut_health_for_reflect(&mut self) -> &mut ::std::option::Option<Membership_Health> {
        &mut self.health
    }
}

impl ::protobuf::Message for Membership {
    fn is_initialized(&self) -> bool {
        for v in &self.member {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.member)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.health = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.member.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(v) = self.health {
            my_size += ::protobuf::rt::enum_size(2, v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.member.as_ref() {
            os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(v) = self.health {
            os.write_enum(2, v.value())?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Membership {
    fn new() -> Membership {
        Membership::new()
    }

    fn descriptor_static(_: ::std::option::Option<Membership>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Member>>(
                    "member",
                    Membership::get_member_for_reflect,
                    Membership::mut_member_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<Membership_Health>>(
                    "health",
                    Membership::get_health_for_reflect,
                    Membership::mut_health_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Membership>(
                    "Membership",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Membership {
    fn clear(&mut self) {
        self.clear_member();
        self.clear_health();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Membership {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Membership {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum Membership_Health {
    ALIVE = 1,
    SUSPECT = 2,
    CONFIRMED = 3,
    DEPARTED = 4,
}

impl ::protobuf::ProtobufEnum for Membership_Health {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<Membership_Health> {
        match value {
            1 => ::std::option::Option::Some(Membership_Health::ALIVE),
            2 => ::std::option::Option::Some(Membership_Health::SUSPECT),
            3 => ::std::option::Option::Some(Membership_Health::CONFIRMED),
            4 => ::std::option::Option::Some(Membership_Health::DEPARTED),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [Membership_Health] = &[
            Membership_Health::ALIVE,
            Membership_Health::SUSPECT,
            Membership_Health::CONFIRMED,
            Membership_Health::DEPARTED,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<Membership_Health>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("Membership_Health", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for Membership_Health {
}

impl ::protobuf::reflect::ProtobufValue for Membership_Health {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Election {
    // message fields
    member_id: ::protobuf::SingularField<::std::string::String>,
    service_group: ::protobuf::SingularField<::std::string::String>,
    term: ::std::option::Option<u64>,
    suitability: ::std::option::Option<u64>,
    status: ::std::option::Option<Election_Status>,
    votes: ::protobuf::RepeatedField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Election {}

impl Election {
    pub fn new() -> Election {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Election {
        static mut instance: ::protobuf::lazy::Lazy<Election> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Election,
        };
        unsafe {
            instance.get(Election::new)
        }
    }

    // optional string member_id = 1;

    pub fn clear_member_id(&mut self) {
        self.member_id.clear();
    }

    pub fn has_member_id(&self) -> bool {
        self.member_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_member_id(&mut self, v: ::std::string::String) {
        self.member_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_member_id(&mut self) -> &mut ::std::string::String {
        if self.member_id.is_none() {
            self.member_id.set_default();
        }
        self.member_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_member_id(&mut self) -> ::std::string::String {
        self.member_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_member_id(&self) -> &str {
        match self.member_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_member_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.member_id
    }

    fn mut_member_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.member_id
    }

    // optional string service_group = 2;

    pub fn clear_service_group(&mut self) {
        self.service_group.clear();
    }

    pub fn has_service_group(&self) -> bool {
        self.service_group.is_some()
    }

    // Param is passed by value, moved
    pub fn set_service_group(&mut self, v: ::std::string::String) {
        self.service_group = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_service_group(&mut self) -> &mut ::std::string::String {
        if self.service_group.is_none() {
            self.service_group.set_default();
        }
        self.service_group.as_mut().unwrap()
    }

    // Take field
    pub fn take_service_group(&mut self) -> ::std::string::String {
        self.service_group.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_service_group(&self) -> &str {
        match self.service_group.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_service_group_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.service_group
    }

    fn mut_service_group_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.service_group
    }

    // optional uint64 term = 3;

    pub fn clear_term(&mut self) {
        self.term = ::std::option::Option::None;
    }

    pub fn has_term(&self) -> bool {
        self.term.is_some()
    }

    // Param is passed by value, moved
    pub fn set_term(&mut self, v: u64) {
        self.term = ::std::option::Option::Some(v);
    }

    pub fn get_term(&self) -> u64 {
        self.term.unwrap_or(0)
    }

    fn get_term_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.term
    }

    fn mut_term_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.term
    }

    // optional uint64 suitability = 4;

    pub fn clear_suitability(&mut self) {
        self.suitability = ::std::option::Option::None;
    }

    pub fn has_suitability(&self) -> bool {
        self.suitability.is_some()
    }

    // Param is passed by value, moved
    pub fn set_suitability(&mut self, v: u64) {
        self.suitability = ::std::option::Option::Some(v);
    }

    pub fn get_suitability(&self) -> u64 {
        self.suitability.unwrap_or(0)
    }

    fn get_suitability_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.suitability
    }

    fn mut_suitability_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.suitability
    }

    // optional .Election.Status status = 5;

    pub fn clear_status(&mut self) {
        self.status = ::std::option::Option::None;
    }

    pub fn has_status(&self) -> bool {
        self.status.is_some()
    }

    // Param is passed by value, moved
    pub fn set_status(&mut self, v: Election_Status) {
        self.status = ::std::option::Option::Some(v);
    }

    pub fn get_status(&self) -> Election_Status {
        self.status.unwrap_or(Election_Status::Running)
    }

    fn get_status_for_reflect(&self) -> &::std::option::Option<Election_Status> {
        &self.status
    }

    fn mut_status_for_reflect(&mut self) -> &mut ::std::option::Option<Election_Status> {
        &mut self.status
    }

    // repeated string votes = 6;

    pub fn clear_votes(&mut self) {
        self.votes.clear();
    }

    // Param is passed by value, moved
    pub fn set_votes(&mut self, v: ::protobuf::RepeatedField<::std::string::String>) {
        self.votes = v;
    }

    // Mutable pointer to the field.
    pub fn mut_votes(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.votes
    }

    // Take field
    pub fn take_votes(&mut self) -> ::protobuf::RepeatedField<::std::string::String> {
        ::std::mem::replace(&mut self.votes, ::protobuf::RepeatedField::new())
    }

    pub fn get_votes(&self) -> &[::std::string::String] {
        &self.votes
    }

    fn get_votes_for_reflect(&self) -> &::protobuf::RepeatedField<::std::string::String> {
        &self.votes
    }

    fn mut_votes_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.votes
    }
}

impl ::protobuf::Message for Election {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.member_id)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.service_group)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.term = ::std::option::Option::Some(tmp);
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.suitability = ::std::option::Option::Some(tmp);
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.status = ::std::option::Option::Some(tmp);
                },
                6 => {
                    ::protobuf::rt::read_repeated_string_into(wire_type, is, &mut self.votes)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.member_id.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(ref v) = self.service_group.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        if let Some(v) = self.term {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.suitability {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.status {
            my_size += ::protobuf::rt::enum_size(5, v);
        }
        for value in &self.votes {
            my_size += ::protobuf::rt::string_size(6, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.member_id.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(ref v) = self.service_group.as_ref() {
            os.write_string(2, &v)?;
        }
        if let Some(v) = self.term {
            os.write_uint64(3, v)?;
        }
        if let Some(v) = self.suitability {
            os.write_uint64(4, v)?;
        }
        if let Some(v) = self.status {
            os.write_enum(5, v.value())?;
        }
        for v in &self.votes {
            os.write_string(6, &v)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Election {
    fn new() -> Election {
        Election::new()
    }

    fn descriptor_static(_: ::std::option::Option<Election>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "member_id",
                    Election::get_member_id_for_reflect,
                    Election::mut_member_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "service_group",
                    Election::get_service_group_for_reflect,
                    Election::mut_service_group_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "term",
                    Election::get_term_for_reflect,
                    Election::mut_term_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "suitability",
                    Election::get_suitability_for_reflect,
                    Election::mut_suitability_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<Election_Status>>(
                    "status",
                    Election::get_status_for_reflect,
                    Election::mut_status_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "votes",
                    Election::get_votes_for_reflect,
                    Election::mut_votes_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Election>(
                    "Election",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Election {
    fn clear(&mut self) {
        self.clear_member_id();
        self.clear_service_group();
        self.clear_term();
        self.clear_suitability();
        self.clear_status();
        self.clear_votes();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Election {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Election {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum Election_Status {
    Running = 1,
    NoQuorum = 2,
    Finished = 3,
}

impl ::protobuf::ProtobufEnum for Election_Status {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<Election_Status> {
        match value {
            1 => ::std::option::Option::Some(Election_Status::Running),
            2 => ::std::option::Option::Some(Election_Status::NoQuorum),
            3 => ::std::option::Option::Some(Election_Status::Finished),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [Election_Status] = &[
            Election_Status::Running,
            Election_Status::NoQuorum,
            Election_Status::Finished,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<Election_Status>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("Election_Status", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for Election_Status {
}

impl ::protobuf::reflect::ProtobufValue for Election_Status {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Service {
    // message fields
    member_id: ::protobuf::SingularField<::std::string::String>,
    service_group: ::protobuf::SingularField<::std::string::String>,
    incarnation: ::std::option::Option<u64>,
    initialized: ::std::option::Option<bool>,
    pkg: ::protobuf::SingularField<::std::string::String>,
    cfg: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    sys: ::protobuf::SingularPtrField<SysInfo>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Service {}

impl Service {
    pub fn new() -> Service {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Service {
        static mut instance: ::protobuf::lazy::Lazy<Service> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Service,
        };
        unsafe {
            instance.get(Service::new)
        }
    }

    // optional string member_id = 1;

    pub fn clear_member_id(&mut self) {
        self.member_id.clear();
    }

    pub fn has_member_id(&self) -> bool {
        self.member_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_member_id(&mut self, v: ::std::string::String) {
        self.member_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_member_id(&mut self) -> &mut ::std::string::String {
        if self.member_id.is_none() {
            self.member_id.set_default();
        }
        self.member_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_member_id(&mut self) -> ::std::string::String {
        self.member_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_member_id(&self) -> &str {
        match self.member_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_member_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.member_id
    }

    fn mut_member_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.member_id
    }

    // optional string service_group = 2;

    pub fn clear_service_group(&mut self) {
        self.service_group.clear();
    }

    pub fn has_service_group(&self) -> bool {
        self.service_group.is_some()
    }

    // Param is passed by value, moved
    pub fn set_service_group(&mut self, v: ::std::string::String) {
        self.service_group = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_service_group(&mut self) -> &mut ::std::string::String {
        if self.service_group.is_none() {
            self.service_group.set_default();
        }
        self.service_group.as_mut().unwrap()
    }

    // Take field
    pub fn take_service_group(&mut self) -> ::std::string::String {
        self.service_group.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_service_group(&self) -> &str {
        match self.service_group.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_service_group_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.service_group
    }

    fn mut_service_group_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.service_group
    }

    // optional uint64 incarnation = 3;

    pub fn clear_incarnation(&mut self) {
        self.incarnation = ::std::option::Option::None;
    }

    pub fn has_incarnation(&self) -> bool {
        self.incarnation.is_some()
    }

    // Param is passed by value, moved
    pub fn set_incarnation(&mut self, v: u64) {
        self.incarnation = ::std::option::Option::Some(v);
    }

    pub fn get_incarnation(&self) -> u64 {
        self.incarnation.unwrap_or(0)
    }

    fn get_incarnation_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.incarnation
    }

    fn mut_incarnation_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.incarnation
    }

    // optional bool initialized = 8;

    pub fn clear_initialized(&mut self) {
        self.initialized = ::std::option::Option::None;
    }

    pub fn has_initialized(&self) -> bool {
        self.initialized.is_some()
    }

    // Param is passed by value, moved
    pub fn set_initialized(&mut self, v: bool) {
        self.initialized = ::std::option::Option::Some(v);
    }

    pub fn get_initialized(&self) -> bool {
        self.initialized.unwrap_or(false)
    }

    fn get_initialized_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.initialized
    }

    fn mut_initialized_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.initialized
    }

    // optional string pkg = 9;

    pub fn clear_pkg(&mut self) {
        self.pkg.clear();
    }

    pub fn has_pkg(&self) -> bool {
        self.pkg.is_some()
    }

    // Param is passed by value, moved
    pub fn set_pkg(&mut self, v: ::std::string::String) {
        self.pkg = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_pkg(&mut self) -> &mut ::std::string::String {
        if self.pkg.is_none() {
            self.pkg.set_default();
        }
        self.pkg.as_mut().unwrap()
    }

    // Take field
    pub fn take_pkg(&mut self) -> ::std::string::String {
        self.pkg.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_pkg(&self) -> &str {
        match self.pkg.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_pkg_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.pkg
    }

    fn mut_pkg_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.pkg
    }

    // optional bytes cfg = 10;

    pub fn clear_cfg(&mut self) {
        self.cfg.clear();
    }

    pub fn has_cfg(&self) -> bool {
        self.cfg.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cfg(&mut self, v: ::std::vec::Vec<u8>) {
        self.cfg = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cfg(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.cfg.is_none() {
            self.cfg.set_default();
        }
        self.cfg.as_mut().unwrap()
    }

    // Take field
    pub fn take_cfg(&mut self) -> ::std::vec::Vec<u8> {
        self.cfg.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_cfg(&self) -> &[u8] {
        match self.cfg.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_cfg_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.cfg
    }

    fn mut_cfg_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.cfg
    }

    // optional .SysInfo sys = 12;

    pub fn clear_sys(&mut self) {
        self.sys.clear();
    }

    pub fn has_sys(&self) -> bool {
        self.sys.is_some()
    }

    // Param is passed by value, moved
    pub fn set_sys(&mut self, v: SysInfo) {
        self.sys = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_sys(&mut self) -> &mut SysInfo {
        if self.sys.is_none() {
            self.sys.set_default();
        }
        self.sys.as_mut().unwrap()
    }

    // Take field
    pub fn take_sys(&mut self) -> SysInfo {
        self.sys.take().unwrap_or_else(|| SysInfo::new())
    }

    pub fn get_sys(&self) -> &SysInfo {
        self.sys.as_ref().unwrap_or_else(|| SysInfo::default_instance())
    }

    fn get_sys_for_reflect(&self) -> &::protobuf::SingularPtrField<SysInfo> {
        &self.sys
    }

    fn mut_sys_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<SysInfo> {
        &mut self.sys
    }
}

impl ::protobuf::Message for Service {
    fn is_initialized(&self) -> bool {
        for v in &self.sys {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.member_id)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.service_group)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.incarnation = ::std::option::Option::Some(tmp);
                },
                8 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.initialized = ::std::option::Option::Some(tmp);
                },
                9 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.pkg)?;
                },
                10 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.cfg)?;
                },
                12 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.sys)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.member_id.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(ref v) = self.service_group.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        if let Some(v) = self.incarnation {
            my_size += ::protobuf::rt::value_size(3, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.initialized {
            my_size += 2;
        }
        if let Some(ref v) = self.pkg.as_ref() {
            my_size += ::protobuf::rt::string_size(9, &v);
        }
        if let Some(ref v) = self.cfg.as_ref() {
            my_size += ::protobuf::rt::bytes_size(10, &v);
        }
        if let Some(ref v) = self.sys.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.member_id.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(ref v) = self.service_group.as_ref() {
            os.write_string(2, &v)?;
        }
        if let Some(v) = self.incarnation {
            os.write_uint64(3, v)?;
        }
        if let Some(v) = self.initialized {
            os.write_bool(8, v)?;
        }
        if let Some(ref v) = self.pkg.as_ref() {
            os.write_string(9, &v)?;
        }
        if let Some(ref v) = self.cfg.as_ref() {
            os.write_bytes(10, &v)?;
        }
        if let Some(ref v) = self.sys.as_ref() {
            os.write_tag(12, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Service {
    fn new() -> Service {
        Service::new()
    }

    fn descriptor_static(_: ::std::option::Option<Service>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "member_id",
                    Service::get_member_id_for_reflect,
                    Service::mut_member_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "service_group",
                    Service::get_service_group_for_reflect,
                    Service::mut_service_group_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "incarnation",
                    Service::get_incarnation_for_reflect,
                    Service::mut_incarnation_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "initialized",
                    Service::get_initialized_for_reflect,
                    Service::mut_initialized_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "pkg",
                    Service::get_pkg_for_reflect,
                    Service::mut_pkg_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "cfg",
                    Service::get_cfg_for_reflect,
                    Service::mut_cfg_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<SysInfo>>(
                    "sys",
                    Service::get_sys_for_reflect,
                    Service::mut_sys_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Service>(
                    "Service",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Service {
    fn clear(&mut self) {
        self.clear_member_id();
        self.clear_service_group();
        self.clear_incarnation();
        self.clear_initialized();
        self.clear_pkg();
        self.clear_cfg();
        self.clear_sys();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Service {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Service {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ServiceConfig {
    // message fields
    service_group: ::protobuf::SingularField<::std::string::String>,
    incarnation: ::std::option::Option<u64>,
    encrypted: ::std::option::Option<bool>,
    config: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ServiceConfig {}

impl ServiceConfig {
    pub fn new() -> ServiceConfig {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ServiceConfig {
        static mut instance: ::protobuf::lazy::Lazy<ServiceConfig> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ServiceConfig,
        };
        unsafe {
            instance.get(ServiceConfig::new)
        }
    }

    // optional string service_group = 1;

    pub fn clear_service_group(&mut self) {
        self.service_group.clear();
    }

    pub fn has_service_group(&self) -> bool {
        self.service_group.is_some()
    }

    // Param is passed by value, moved
    pub fn set_service_group(&mut self, v: ::std::string::String) {
        self.service_group = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_service_group(&mut self) -> &mut ::std::string::String {
        if self.service_group.is_none() {
            self.service_group.set_default();
        }
        self.service_group.as_mut().unwrap()
    }

    // Take field
    pub fn take_service_group(&mut self) -> ::std::string::String {
        self.service_group.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_service_group(&self) -> &str {
        match self.service_group.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_service_group_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.service_group
    }

    fn mut_service_group_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.service_group
    }

    // optional uint64 incarnation = 2;

    pub fn clear_incarnation(&mut self) {
        self.incarnation = ::std::option::Option::None;
    }

    pub fn has_incarnation(&self) -> bool {
        self.incarnation.is_some()
    }

    // Param is passed by value, moved
    pub fn set_incarnation(&mut self, v: u64) {
        self.incarnation = ::std::option::Option::Some(v);
    }

    pub fn get_incarnation(&self) -> u64 {
        self.incarnation.unwrap_or(0)
    }

    fn get_incarnation_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.incarnation
    }

    fn mut_incarnation_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.incarnation
    }

    // optional bool encrypted = 3;

    pub fn clear_encrypted(&mut self) {
        self.encrypted = ::std::option::Option::None;
    }

    pub fn has_encrypted(&self) -> bool {
        self.encrypted.is_some()
    }

    // Param is passed by value, moved
    pub fn set_encrypted(&mut self, v: bool) {
        self.encrypted = ::std::option::Option::Some(v);
    }

    pub fn get_encrypted(&self) -> bool {
        self.encrypted.unwrap_or(false)
    }

    fn get_encrypted_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.encrypted
    }

    fn mut_encrypted_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.encrypted
    }

    // optional bytes config = 4;

    pub fn clear_config(&mut self) {
        self.config.clear();
    }

    pub fn has_config(&self) -> bool {
        self.config.is_some()
    }

    // Param is passed by value, moved
    pub fn set_config(&mut self, v: ::std::vec::Vec<u8>) {
        self.config = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_config(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.config.is_none() {
            self.config.set_default();
        }
        self.config.as_mut().unwrap()
    }

    // Take field
    pub fn take_config(&mut self) -> ::std::vec::Vec<u8> {
        self.config.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_config(&self) -> &[u8] {
        match self.config.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_config_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.config
    }

    fn mut_config_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.config
    }
}

impl ::protobuf::Message for ServiceConfig {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.service_group)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.incarnation = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.encrypted = ::std::option::Option::Some(tmp);
                },
                4 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.config)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.service_group.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(v) = self.incarnation {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.encrypted {
            my_size += 2;
        }
        if let Some(ref v) = self.config.as_ref() {
            my_size += ::protobuf::rt::bytes_size(4, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.service_group.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(v) = self.incarnation {
            os.write_uint64(2, v)?;
        }
        if let Some(v) = self.encrypted {
            os.write_bool(3, v)?;
        }
        if let Some(ref v) = self.config.as_ref() {
            os.write_bytes(4, &v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ServiceConfig {
    fn new() -> ServiceConfig {
        ServiceConfig::new()
    }

    fn descriptor_static(_: ::std::option::Option<ServiceConfig>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "service_group",
                    ServiceConfig::get_service_group_for_reflect,
                    ServiceConfig::mut_service_group_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "incarnation",
                    ServiceConfig::get_incarnation_for_reflect,
                    ServiceConfig::mut_incarnation_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "encrypted",
                    ServiceConfig::get_encrypted_for_reflect,
                    ServiceConfig::mut_encrypted_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "config",
                    ServiceConfig::get_config_for_reflect,
                    ServiceConfig::mut_config_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ServiceConfig>(
                    "ServiceConfig",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ServiceConfig {
    fn clear(&mut self) {
        self.clear_service_group();
        self.clear_incarnation();
        self.clear_encrypted();
        self.clear_config();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ServiceConfig {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ServiceConfig {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct ServiceFile {
    // message fields
    service_group: ::protobuf::SingularField<::std::string::String>,
    incarnation: ::std::option::Option<u64>,
    encrypted: ::std::option::Option<bool>,
    filename: ::protobuf::SingularField<::std::string::String>,
    body: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ServiceFile {}

impl ServiceFile {
    pub fn new() -> ServiceFile {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ServiceFile {
        static mut instance: ::protobuf::lazy::Lazy<ServiceFile> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ServiceFile,
        };
        unsafe {
            instance.get(ServiceFile::new)
        }
    }

    // optional string service_group = 1;

    pub fn clear_service_group(&mut self) {
        self.service_group.clear();
    }

    pub fn has_service_group(&self) -> bool {
        self.service_group.is_some()
    }

    // Param is passed by value, moved
    pub fn set_service_group(&mut self, v: ::std::string::String) {
        self.service_group = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_service_group(&mut self) -> &mut ::std::string::String {
        if self.service_group.is_none() {
            self.service_group.set_default();
        }
        self.service_group.as_mut().unwrap()
    }

    // Take field
    pub fn take_service_group(&mut self) -> ::std::string::String {
        self.service_group.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_service_group(&self) -> &str {
        match self.service_group.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_service_group_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.service_group
    }

    fn mut_service_group_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.service_group
    }

    // optional uint64 incarnation = 2;

    pub fn clear_incarnation(&mut self) {
        self.incarnation = ::std::option::Option::None;
    }

    pub fn has_incarnation(&self) -> bool {
        self.incarnation.is_some()
    }

    // Param is passed by value, moved
    pub fn set_incarnation(&mut self, v: u64) {
        self.incarnation = ::std::option::Option::Some(v);
    }

    pub fn get_incarnation(&self) -> u64 {
        self.incarnation.unwrap_or(0)
    }

    fn get_incarnation_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.incarnation
    }

    fn mut_incarnation_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.incarnation
    }

    // optional bool encrypted = 3;

    pub fn clear_encrypted(&mut self) {
        self.encrypted = ::std::option::Option::None;
    }

    pub fn has_encrypted(&self) -> bool {
        self.encrypted.is_some()
    }

    // Param is passed by value, moved
    pub fn set_encrypted(&mut self, v: bool) {
        self.encrypted = ::std::option::Option::Some(v);
    }

    pub fn get_encrypted(&self) -> bool {
        self.encrypted.unwrap_or(false)
    }

    fn get_encrypted_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.encrypted
    }

    fn mut_encrypted_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.encrypted
    }

    // optional string filename = 4;

    pub fn clear_filename(&mut self) {
        self.filename.clear();
    }

    pub fn has_filename(&self) -> bool {
        self.filename.is_some()
    }

    // Param is passed by value, moved
    pub fn set_filename(&mut self, v: ::std::string::String) {
        self.filename = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_filename(&mut self) -> &mut ::std::string::String {
        if self.filename.is_none() {
            self.filename.set_default();
        }
        self.filename.as_mut().unwrap()
    }

    // Take field
    pub fn take_filename(&mut self) -> ::std::string::String {
        self.filename.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_filename(&self) -> &str {
        match self.filename.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_filename_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.filename
    }

    fn mut_filename_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.filename
    }

    // optional bytes body = 5;

    pub fn clear_body(&mut self) {
        self.body.clear();
    }

    pub fn has_body(&self) -> bool {
        self.body.is_some()
    }

    // Param is passed by value, moved
    pub fn set_body(&mut self, v: ::std::vec::Vec<u8>) {
        self.body = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_body(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.body.is_none() {
            self.body.set_default();
        }
        self.body.as_mut().unwrap()
    }

    // Take field
    pub fn take_body(&mut self) -> ::std::vec::Vec<u8> {
        self.body.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_body(&self) -> &[u8] {
        match self.body.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_body_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.body
    }

    fn mut_body_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.body
    }
}

impl ::protobuf::Message for ServiceFile {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.service_group)?;
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.incarnation = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.encrypted = ::std::option::Option::Some(tmp);
                },
                4 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.filename)?;
                },
                5 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.body)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.service_group.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(v) = self.incarnation {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.encrypted {
            my_size += 2;
        }
        if let Some(ref v) = self.filename.as_ref() {
            my_size += ::protobuf::rt::string_size(4, &v);
        }
        if let Some(ref v) = self.body.as_ref() {
            my_size += ::protobuf::rt::bytes_size(5, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.service_group.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(v) = self.incarnation {
            os.write_uint64(2, v)?;
        }
        if let Some(v) = self.encrypted {
            os.write_bool(3, v)?;
        }
        if let Some(ref v) = self.filename.as_ref() {
            os.write_string(4, &v)?;
        }
        if let Some(ref v) = self.body.as_ref() {
            os.write_bytes(5, &v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ServiceFile {
    fn new() -> ServiceFile {
        ServiceFile::new()
    }

    fn descriptor_static(_: ::std::option::Option<ServiceFile>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "service_group",
                    ServiceFile::get_service_group_for_reflect,
                    ServiceFile::mut_service_group_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "incarnation",
                    ServiceFile::get_incarnation_for_reflect,
                    ServiceFile::mut_incarnation_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "encrypted",
                    ServiceFile::get_encrypted_for_reflect,
                    ServiceFile::mut_encrypted_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "filename",
                    ServiceFile::get_filename_for_reflect,
                    ServiceFile::mut_filename_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "body",
                    ServiceFile::get_body_for_reflect,
                    ServiceFile::mut_body_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ServiceFile>(
                    "ServiceFile",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ServiceFile {
    fn clear(&mut self) {
        self.clear_service_group();
        self.clear_incarnation();
        self.clear_encrypted();
        self.clear_filename();
        self.clear_body();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for ServiceFile {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for ServiceFile {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct SysInfo {
    // message fields
    ip: ::protobuf::SingularField<::std::string::String>,
    hostname: ::protobuf::SingularField<::std::string::String>,
    gossip_ip: ::protobuf::SingularField<::std::string::String>,
    gossip_port: ::std::option::Option<u32>,
    http_gateway_ip: ::protobuf::SingularField<::std::string::String>,
    http_gateway_port: ::std::option::Option<u32>,
    ctl_gateway_ip: ::protobuf::SingularField<::std::string::String>,
    ctl_gateway_port: ::std::option::Option<u32>,
    additional_addresses: ::protobuf::RepeatedField<ZoneAddress>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SysInfo {}

impl SysInfo {
    pub fn new() -> SysInfo {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SysInfo {
        static mut instance: ::protobuf::lazy::Lazy<SysInfo> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SysInfo,
        };
        unsafe {
            instance.get(SysInfo::new)
        }
    }

    // optional string ip = 1;

    pub fn clear_ip(&mut self) {
        self.ip.clear();
    }

    pub fn has_ip(&self) -> bool {
        self.ip.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ip(&mut self, v: ::std::string::String) {
        self.ip = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_ip(&mut self) -> &mut ::std::string::String {
        if self.ip.is_none() {
            self.ip.set_default();
        }
        self.ip.as_mut().unwrap()
    }

    // Take field
    pub fn take_ip(&mut self) -> ::std::string::String {
        self.ip.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_ip(&self) -> &str {
        match self.ip.as_ref() {
            Some(v) => &v,
            None => "127.0.0.1",
        }
    }

    fn get_ip_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.ip
    }

    fn mut_ip_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.ip
    }

    // optional string hostname = 2;

    pub fn clear_hostname(&mut self) {
        self.hostname.clear();
    }

    pub fn has_hostname(&self) -> bool {
        self.hostname.is_some()
    }

    // Param is passed by value, moved
    pub fn set_hostname(&mut self, v: ::std::string::String) {
        self.hostname = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_hostname(&mut self) -> &mut ::std::string::String {
        if self.hostname.is_none() {
            self.hostname.set_default();
        }
        self.hostname.as_mut().unwrap()
    }

    // Take field
    pub fn take_hostname(&mut self) -> ::std::string::String {
        self.hostname.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_hostname(&self) -> &str {
        match self.hostname.as_ref() {
            Some(v) => &v,
            None => "localhost",
        }
    }

    fn get_hostname_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.hostname
    }

    fn mut_hostname_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.hostname
    }

    // optional string gossip_ip = 3;

    pub fn clear_gossip_ip(&mut self) {
        self.gossip_ip.clear();
    }

    pub fn has_gossip_ip(&self) -> bool {
        self.gossip_ip.is_some()
    }

    // Param is passed by value, moved
    pub fn set_gossip_ip(&mut self, v: ::std::string::String) {
        self.gossip_ip = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_gossip_ip(&mut self) -> &mut ::std::string::String {
        if self.gossip_ip.is_none() {
            self.gossip_ip.set_default();
        }
        self.gossip_ip.as_mut().unwrap()
    }

    // Take field
    pub fn take_gossip_ip(&mut self) -> ::std::string::String {
        self.gossip_ip.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_gossip_ip(&self) -> &str {
        match self.gossip_ip.as_ref() {
            Some(v) => &v,
            None => "127.0.0.1",
        }
    }

    fn get_gossip_ip_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.gossip_ip
    }

    fn mut_gossip_ip_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.gossip_ip
    }

    // optional uint32 gossip_port = 4;

    pub fn clear_gossip_port(&mut self) {
        self.gossip_port = ::std::option::Option::None;
    }

    pub fn has_gossip_port(&self) -> bool {
        self.gossip_port.is_some()
    }

    // Param is passed by value, moved
    pub fn set_gossip_port(&mut self, v: u32) {
        self.gossip_port = ::std::option::Option::Some(v);
    }

    pub fn get_gossip_port(&self) -> u32 {
        self.gossip_port.unwrap_or(0)
    }

    fn get_gossip_port_for_reflect(&self) -> &::std::option::Option<u32> {
        &self.gossip_port
    }

    fn mut_gossip_port_for_reflect(&mut self) -> &mut ::std::option::Option<u32> {
        &mut self.gossip_port
    }

    // optional string http_gateway_ip = 5;

    pub fn clear_http_gateway_ip(&mut self) {
        self.http_gateway_ip.clear();
    }

    pub fn has_http_gateway_ip(&self) -> bool {
        self.http_gateway_ip.is_some()
    }

    // Param is passed by value, moved
    pub fn set_http_gateway_ip(&mut self, v: ::std::string::String) {
        self.http_gateway_ip = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_http_gateway_ip(&mut self) -> &mut ::std::string::String {
        if self.http_gateway_ip.is_none() {
            self.http_gateway_ip.set_default();
        }
        self.http_gateway_ip.as_mut().unwrap()
    }

    // Take field
    pub fn take_http_gateway_ip(&mut self) -> ::std::string::String {
        self.http_gateway_ip.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_http_gateway_ip(&self) -> &str {
        match self.http_gateway_ip.as_ref() {
            Some(v) => &v,
            None => "127.0.0.1",
        }
    }

    fn get_http_gateway_ip_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.http_gateway_ip
    }

    fn mut_http_gateway_ip_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.http_gateway_ip
    }

    // optional uint32 http_gateway_port = 6;

    pub fn clear_http_gateway_port(&mut self) {
        self.http_gateway_port = ::std::option::Option::None;
    }

    pub fn has_http_gateway_port(&self) -> bool {
        self.http_gateway_port.is_some()
    }

    // Param is passed by value, moved
    pub fn set_http_gateway_port(&mut self, v: u32) {
        self.http_gateway_port = ::std::option::Option::Some(v);
    }

    pub fn get_http_gateway_port(&self) -> u32 {
        self.http_gateway_port.unwrap_or(0)
    }

    fn get_http_gateway_port_for_reflect(&self) -> &::std::option::Option<u32> {
        &self.http_gateway_port
    }

    fn mut_http_gateway_port_for_reflect(&mut self) -> &mut ::std::option::Option<u32> {
        &mut self.http_gateway_port
    }

    // optional string ctl_gateway_ip = 7;

    pub fn clear_ctl_gateway_ip(&mut self) {
        self.ctl_gateway_ip.clear();
    }

    pub fn has_ctl_gateway_ip(&self) -> bool {
        self.ctl_gateway_ip.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ctl_gateway_ip(&mut self, v: ::std::string::String) {
        self.ctl_gateway_ip = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_ctl_gateway_ip(&mut self) -> &mut ::std::string::String {
        if self.ctl_gateway_ip.is_none() {
            self.ctl_gateway_ip.set_default();
        }
        self.ctl_gateway_ip.as_mut().unwrap()
    }

    // Take field
    pub fn take_ctl_gateway_ip(&mut self) -> ::std::string::String {
        self.ctl_gateway_ip.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_ctl_gateway_ip(&self) -> &str {
        match self.ctl_gateway_ip.as_ref() {
            Some(v) => &v,
            None => "127.0.0.1",
        }
    }

    fn get_ctl_gateway_ip_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.ctl_gateway_ip
    }

    fn mut_ctl_gateway_ip_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.ctl_gateway_ip
    }

    // optional uint32 ctl_gateway_port = 8;

    pub fn clear_ctl_gateway_port(&mut self) {
        self.ctl_gateway_port = ::std::option::Option::None;
    }

    pub fn has_ctl_gateway_port(&self) -> bool {
        self.ctl_gateway_port.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ctl_gateway_port(&mut self, v: u32) {
        self.ctl_gateway_port = ::std::option::Option::Some(v);
    }

    pub fn get_ctl_gateway_port(&self) -> u32 {
        self.ctl_gateway_port.unwrap_or(9632u32)
    }

    fn get_ctl_gateway_port_for_reflect(&self) -> &::std::option::Option<u32> {
        &self.ctl_gateway_port
    }

    fn mut_ctl_gateway_port_for_reflect(&mut self) -> &mut ::std::option::Option<u32> {
        &mut self.ctl_gateway_port
    }

    // repeated .ZoneAddress additional_addresses = 9;

    pub fn clear_additional_addresses(&mut self) {
        self.additional_addresses.clear();
    }

    // Param is passed by value, moved
    pub fn set_additional_addresses(&mut self, v: ::protobuf::RepeatedField<ZoneAddress>) {
        self.additional_addresses = v;
    }

    // Mutable pointer to the field.
    pub fn mut_additional_addresses(&mut self) -> &mut ::protobuf::RepeatedField<ZoneAddress> {
        &mut self.additional_addresses
    }

    // Take field
    pub fn take_additional_addresses(&mut self) -> ::protobuf::RepeatedField<ZoneAddress> {
        ::std::mem::replace(&mut self.additional_addresses, ::protobuf::RepeatedField::new())
    }

    pub fn get_additional_addresses(&self) -> &[ZoneAddress] {
        &self.additional_addresses
    }

    fn get_additional_addresses_for_reflect(&self) -> &::protobuf::RepeatedField<ZoneAddress> {
        &self.additional_addresses
    }

    fn mut_additional_addresses_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<ZoneAddress> {
        &mut self.additional_addresses
    }
}

impl ::protobuf::Message for SysInfo {
    fn is_initialized(&self) -> bool {
        for v in &self.additional_addresses {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.ip)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.hostname)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.gossip_ip)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.gossip_port = ::std::option::Option::Some(tmp);
                },
                5 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.http_gateway_ip)?;
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.http_gateway_port = ::std::option::Option::Some(tmp);
                },
                7 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.ctl_gateway_ip)?;
                },
                8 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.ctl_gateway_port = ::std::option::Option::Some(tmp);
                },
                9 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.additional_addresses)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.ip.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(ref v) = self.hostname.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        if let Some(ref v) = self.gossip_ip.as_ref() {
            my_size += ::protobuf::rt::string_size(3, &v);
        }
        if let Some(v) = self.gossip_port {
            my_size += ::protobuf::rt::value_size(4, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.http_gateway_ip.as_ref() {
            my_size += ::protobuf::rt::string_size(5, &v);
        }
        if let Some(v) = self.http_gateway_port {
            my_size += ::protobuf::rt::value_size(6, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.ctl_gateway_ip.as_ref() {
            my_size += ::protobuf::rt::string_size(7, &v);
        }
        if let Some(v) = self.ctl_gateway_port {
            my_size += ::protobuf::rt::value_size(8, v, ::protobuf::wire_format::WireTypeVarint);
        }
        for value in &self.additional_addresses {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.ip.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(ref v) = self.hostname.as_ref() {
            os.write_string(2, &v)?;
        }
        if let Some(ref v) = self.gossip_ip.as_ref() {
            os.write_string(3, &v)?;
        }
        if let Some(v) = self.gossip_port {
            os.write_uint32(4, v)?;
        }
        if let Some(ref v) = self.http_gateway_ip.as_ref() {
            os.write_string(5, &v)?;
        }
        if let Some(v) = self.http_gateway_port {
            os.write_uint32(6, v)?;
        }
        if let Some(ref v) = self.ctl_gateway_ip.as_ref() {
            os.write_string(7, &v)?;
        }
        if let Some(v) = self.ctl_gateway_port {
            os.write_uint32(8, v)?;
        }
        for v in &self.additional_addresses {
            os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for SysInfo {
    fn new() -> SysInfo {
        SysInfo::new()
    }

    fn descriptor_static(_: ::std::option::Option<SysInfo>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "ip",
                    SysInfo::get_ip_for_reflect,
                    SysInfo::mut_ip_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "hostname",
                    SysInfo::get_hostname_for_reflect,
                    SysInfo::mut_hostname_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "gossip_ip",
                    SysInfo::get_gossip_ip_for_reflect,
                    SysInfo::mut_gossip_ip_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "gossip_port",
                    SysInfo::get_gossip_port_for_reflect,
                    SysInfo::mut_gossip_port_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "http_gateway_ip",
                    SysInfo::get_http_gateway_ip_for_reflect,
                    SysInfo::mut_http_gateway_ip_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "http_gateway_port",
                    SysInfo::get_http_gateway_port_for_reflect,
                    SysInfo::mut_http_gateway_port_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "ctl_gateway_ip",
                    SysInfo::get_ctl_gateway_ip_for_reflect,
                    SysInfo::mut_ctl_gateway_ip_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "ctl_gateway_port",
                    SysInfo::get_ctl_gateway_port_for_reflect,
                    SysInfo::mut_ctl_gateway_port_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<ZoneAddress>>(
                    "additional_addresses",
                    SysInfo::get_additional_addresses_for_reflect,
                    SysInfo::mut_additional_addresses_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SysInfo>(
                    "SysInfo",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SysInfo {
    fn clear(&mut self) {
        self.clear_ip();
        self.clear_hostname();
        self.clear_gossip_ip();
        self.clear_gossip_port();
        self.clear_http_gateway_ip();
        self.clear_http_gateway_port();
        self.clear_ctl_gateway_ip();
        self.clear_ctl_gateway_port();
        self.clear_additional_addresses();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for SysInfo {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for SysInfo {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Departure {
    // message fields
    member_id: ::protobuf::SingularField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Departure {}

impl Departure {
    pub fn new() -> Departure {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Departure {
        static mut instance: ::protobuf::lazy::Lazy<Departure> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Departure,
        };
        unsafe {
            instance.get(Departure::new)
        }
    }

    // optional string member_id = 1;

    pub fn clear_member_id(&mut self) {
        self.member_id.clear();
    }

    pub fn has_member_id(&self) -> bool {
        self.member_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_member_id(&mut self, v: ::std::string::String) {
        self.member_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_member_id(&mut self) -> &mut ::std::string::String {
        if self.member_id.is_none() {
            self.member_id.set_default();
        }
        self.member_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_member_id(&mut self) -> ::std::string::String {
        self.member_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_member_id(&self) -> &str {
        match self.member_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_member_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.member_id
    }

    fn mut_member_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.member_id
    }
}

impl ::protobuf::Message for Departure {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.member_id)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.member_id.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.member_id.as_ref() {
            os.write_string(1, &v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Departure {
    fn new() -> Departure {
        Departure::new()
    }

    fn descriptor_static(_: ::std::option::Option<Departure>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "member_id",
                    Departure::get_member_id_for_reflect,
                    Departure::mut_member_id_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Departure>(
                    "Departure",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Departure {
    fn clear(&mut self) {
        self.clear_member_id();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Departure {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Departure {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Swim {
    // message fields
    field_type: ::std::option::Option<Swim_Type>,
    membership: ::protobuf::RepeatedField<Membership>,
    zones: ::protobuf::RepeatedField<Zone>,
    // message oneof groups
    payload: ::std::option::Option<Swim_oneof_payload>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Swim {}

#[derive(Clone,PartialEq)]
pub enum Swim_oneof_payload {
    ping(Ping),
    ack(Ack),
    pingreq(PingReq),
    zone_change(ZoneChange),
}

impl Swim {
    pub fn new() -> Swim {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Swim {
        static mut instance: ::protobuf::lazy::Lazy<Swim> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Swim,
        };
        unsafe {
            instance.get(Swim::new)
        }
    }

    // required .Swim.Type type = 1;

    pub fn clear_field_type(&mut self) {
        self.field_type = ::std::option::Option::None;
    }

    pub fn has_field_type(&self) -> bool {
        self.field_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: Swim_Type) {
        self.field_type = ::std::option::Option::Some(v);
    }

    pub fn get_field_type(&self) -> Swim_Type {
        self.field_type.unwrap_or(Swim_Type::PING)
    }

    fn get_field_type_for_reflect(&self) -> &::std::option::Option<Swim_Type> {
        &self.field_type
    }

    fn mut_field_type_for_reflect(&mut self) -> &mut ::std::option::Option<Swim_Type> {
        &mut self.field_type
    }

    // optional .Ping ping = 2;

    pub fn clear_ping(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_ping(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::ping(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_ping(&mut self, v: Ping) {
        self.payload = ::std::option::Option::Some(Swim_oneof_payload::ping(v))
    }

    // Mutable pointer to the field.
    pub fn mut_ping(&mut self) -> &mut Ping {
        if let ::std::option::Option::Some(Swim_oneof_payload::ping(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Swim_oneof_payload::ping(Ping::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::ping(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_ping(&mut self) -> Ping {
        if self.has_ping() {
            match self.payload.take() {
                ::std::option::Option::Some(Swim_oneof_payload::ping(v)) => v,
                _ => panic!(),
            }
        } else {
            Ping::new()
        }
    }

    pub fn get_ping(&self) -> &Ping {
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::ping(ref v)) => v,
            _ => Ping::default_instance(),
        }
    }

    // optional .Ack ack = 3;

    pub fn clear_ack(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_ack(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::ack(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_ack(&mut self, v: Ack) {
        self.payload = ::std::option::Option::Some(Swim_oneof_payload::ack(v))
    }

    // Mutable pointer to the field.
    pub fn mut_ack(&mut self) -> &mut Ack {
        if let ::std::option::Option::Some(Swim_oneof_payload::ack(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Swim_oneof_payload::ack(Ack::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::ack(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_ack(&mut self) -> Ack {
        if self.has_ack() {
            match self.payload.take() {
                ::std::option::Option::Some(Swim_oneof_payload::ack(v)) => v,
                _ => panic!(),
            }
        } else {
            Ack::new()
        }
    }

    pub fn get_ack(&self) -> &Ack {
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::ack(ref v)) => v,
            _ => Ack::default_instance(),
        }
    }

    // optional .PingReq pingreq = 4;

    pub fn clear_pingreq(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_pingreq(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::pingreq(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_pingreq(&mut self, v: PingReq) {
        self.payload = ::std::option::Option::Some(Swim_oneof_payload::pingreq(v))
    }

    // Mutable pointer to the field.
    pub fn mut_pingreq(&mut self) -> &mut PingReq {
        if let ::std::option::Option::Some(Swim_oneof_payload::pingreq(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Swim_oneof_payload::pingreq(PingReq::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::pingreq(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_pingreq(&mut self) -> PingReq {
        if self.has_pingreq() {
            match self.payload.take() {
                ::std::option::Option::Some(Swim_oneof_payload::pingreq(v)) => v,
                _ => panic!(),
            }
        } else {
            PingReq::new()
        }
    }

    pub fn get_pingreq(&self) -> &PingReq {
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::pingreq(ref v)) => v,
            _ => PingReq::default_instance(),
        }
    }

    // optional .ZoneChange zone_change = 7;

    pub fn clear_zone_change(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_zone_change(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::zone_change(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_zone_change(&mut self, v: ZoneChange) {
        self.payload = ::std::option::Option::Some(Swim_oneof_payload::zone_change(v))
    }

    // Mutable pointer to the field.
    pub fn mut_zone_change(&mut self) -> &mut ZoneChange {
        if let ::std::option::Option::Some(Swim_oneof_payload::zone_change(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Swim_oneof_payload::zone_change(ZoneChange::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::zone_change(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_zone_change(&mut self) -> ZoneChange {
        if self.has_zone_change() {
            match self.payload.take() {
                ::std::option::Option::Some(Swim_oneof_payload::zone_change(v)) => v,
                _ => panic!(),
            }
        } else {
            ZoneChange::new()
        }
    }

    pub fn get_zone_change(&self) -> &ZoneChange {
        match self.payload {
            ::std::option::Option::Some(Swim_oneof_payload::zone_change(ref v)) => v,
            _ => ZoneChange::default_instance(),
        }
    }

    // repeated .Membership membership = 5;

    pub fn clear_membership(&mut self) {
        self.membership.clear();
    }

    // Param is passed by value, moved
    pub fn set_membership(&mut self, v: ::protobuf::RepeatedField<Membership>) {
        self.membership = v;
    }

    // Mutable pointer to the field.
    pub fn mut_membership(&mut self) -> &mut ::protobuf::RepeatedField<Membership> {
        &mut self.membership
    }

    // Take field
    pub fn take_membership(&mut self) -> ::protobuf::RepeatedField<Membership> {
        ::std::mem::replace(&mut self.membership, ::protobuf::RepeatedField::new())
    }

    pub fn get_membership(&self) -> &[Membership] {
        &self.membership
    }

    fn get_membership_for_reflect(&self) -> &::protobuf::RepeatedField<Membership> {
        &self.membership
    }

    fn mut_membership_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Membership> {
        &mut self.membership
    }

    // repeated .Zone zones = 6;

    pub fn clear_zones(&mut self) {
        self.zones.clear();
    }

    // Param is passed by value, moved
    pub fn set_zones(&mut self, v: ::protobuf::RepeatedField<Zone>) {
        self.zones = v;
    }

    // Mutable pointer to the field.
    pub fn mut_zones(&mut self) -> &mut ::protobuf::RepeatedField<Zone> {
        &mut self.zones
    }

    // Take field
    pub fn take_zones(&mut self) -> ::protobuf::RepeatedField<Zone> {
        ::std::mem::replace(&mut self.zones, ::protobuf::RepeatedField::new())
    }

    pub fn get_zones(&self) -> &[Zone] {
        &self.zones
    }

    fn get_zones_for_reflect(&self) -> &::protobuf::RepeatedField<Zone> {
        &self.zones
    }

    fn mut_zones_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Zone> {
        &mut self.zones
    }
}

impl ::protobuf::Message for Swim {
    fn is_initialized(&self) -> bool {
        if self.field_type.is_none() {
            return false;
        }
        if let Some(Swim_oneof_payload::ping(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Swim_oneof_payload::ack(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Swim_oneof_payload::pingreq(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Swim_oneof_payload::zone_change(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        for v in &self.membership {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.zones {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.field_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Swim_oneof_payload::ping(is.read_message()?));
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Swim_oneof_payload::ack(is.read_message()?));
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Swim_oneof_payload::pingreq(is.read_message()?));
                },
                7 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Swim_oneof_payload::zone_change(is.read_message()?));
                },
                5 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.membership)?;
                },
                6 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.zones)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.field_type {
            my_size += ::protobuf::rt::enum_size(1, v);
        }
        for value in &self.membership {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.zones {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if let ::std::option::Option::Some(ref v) = self.payload {
            match v {
                &Swim_oneof_payload::ping(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Swim_oneof_payload::ack(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Swim_oneof_payload::pingreq(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Swim_oneof_payload::zone_change(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
            };
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.field_type {
            os.write_enum(1, v.value())?;
        }
        for v in &self.membership {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        for v in &self.zones {
            os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        if let ::std::option::Option::Some(ref v) = self.payload {
            match v {
                &Swim_oneof_payload::ping(ref v) => {
                    os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Swim_oneof_payload::ack(ref v) => {
                    os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Swim_oneof_payload::pingreq(ref v) => {
                    os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Swim_oneof_payload::zone_change(ref v) => {
                    os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
            };
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Swim {
    fn new() -> Swim {
        Swim::new()
    }

    fn descriptor_static(_: ::std::option::Option<Swim>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<Swim_Type>>(
                    "type",
                    Swim::get_field_type_for_reflect,
                    Swim::mut_field_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, Ping>(
                    "ping",
                    Swim::has_ping,
                    Swim::get_ping,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, Ack>(
                    "ack",
                    Swim::has_ack,
                    Swim::get_ack,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, PingReq>(
                    "pingreq",
                    Swim::has_pingreq,
                    Swim::get_pingreq,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, ZoneChange>(
                    "zone_change",
                    Swim::has_zone_change,
                    Swim::get_zone_change,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Membership>>(
                    "membership",
                    Swim::get_membership_for_reflect,
                    Swim::mut_membership_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Zone>>(
                    "zones",
                    Swim::get_zones_for_reflect,
                    Swim::mut_zones_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Swim>(
                    "Swim",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Swim {
    fn clear(&mut self) {
        self.clear_field_type();
        self.clear_ping();
        self.clear_ack();
        self.clear_pingreq();
        self.clear_zone_change();
        self.clear_membership();
        self.clear_zones();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Swim {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Swim {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum Swim_Type {
    PING = 1,
    ACK = 2,
    PINGREQ = 3,
    ZONE_CHANGE = 4,
}

impl ::protobuf::ProtobufEnum for Swim_Type {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<Swim_Type> {
        match value {
            1 => ::std::option::Option::Some(Swim_Type::PING),
            2 => ::std::option::Option::Some(Swim_Type::ACK),
            3 => ::std::option::Option::Some(Swim_Type::PINGREQ),
            4 => ::std::option::Option::Some(Swim_Type::ZONE_CHANGE),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [Swim_Type] = &[
            Swim_Type::PING,
            Swim_Type::ACK,
            Swim_Type::PINGREQ,
            Swim_Type::ZONE_CHANGE,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<Swim_Type>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("Swim_Type", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for Swim_Type {
}

impl ::protobuf::reflect::ProtobufValue for Swim_Type {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Rumor {
    // message fields
    field_type: ::std::option::Option<Rumor_Type>,
    tag: ::protobuf::RepeatedField<::std::string::String>,
    from_id: ::protobuf::SingularField<::std::string::String>,
    // message oneof groups
    payload: ::std::option::Option<Rumor_oneof_payload>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Rumor {}

#[derive(Clone,PartialEq)]
pub enum Rumor_oneof_payload {
    member(Membership),
    service(Service),
    service_config(ServiceConfig),
    service_file(ServiceFile),
    election(Election),
    departure(Departure),
    zone(Zone),
}

impl Rumor {
    pub fn new() -> Rumor {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Rumor {
        static mut instance: ::protobuf::lazy::Lazy<Rumor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Rumor,
        };
        unsafe {
            instance.get(Rumor::new)
        }
    }

    // required .Rumor.Type type = 1;

    pub fn clear_field_type(&mut self) {
        self.field_type = ::std::option::Option::None;
    }

    pub fn has_field_type(&self) -> bool {
        self.field_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: Rumor_Type) {
        self.field_type = ::std::option::Option::Some(v);
    }

    pub fn get_field_type(&self) -> Rumor_Type {
        self.field_type.unwrap_or(Rumor_Type::Member)
    }

    fn get_field_type_for_reflect(&self) -> &::std::option::Option<Rumor_Type> {
        &self.field_type
    }

    fn mut_field_type_for_reflect(&mut self) -> &mut ::std::option::Option<Rumor_Type> {
        &mut self.field_type
    }

    // repeated string tag = 2;

    pub fn clear_tag(&mut self) {
        self.tag.clear();
    }

    // Param is passed by value, moved
    pub fn set_tag(&mut self, v: ::protobuf::RepeatedField<::std::string::String>) {
        self.tag = v;
    }

    // Mutable pointer to the field.
    pub fn mut_tag(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.tag
    }

    // Take field
    pub fn take_tag(&mut self) -> ::protobuf::RepeatedField<::std::string::String> {
        ::std::mem::replace(&mut self.tag, ::protobuf::RepeatedField::new())
    }

    pub fn get_tag(&self) -> &[::std::string::String] {
        &self.tag
    }

    fn get_tag_for_reflect(&self) -> &::protobuf::RepeatedField<::std::string::String> {
        &self.tag
    }

    fn mut_tag_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::string::String> {
        &mut self.tag
    }

    // optional string from_id = 3;

    pub fn clear_from_id(&mut self) {
        self.from_id.clear();
    }

    pub fn has_from_id(&self) -> bool {
        self.from_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_from_id(&mut self, v: ::std::string::String) {
        self.from_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_from_id(&mut self) -> &mut ::std::string::String {
        if self.from_id.is_none() {
            self.from_id.set_default();
        }
        self.from_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_from_id(&mut self) -> ::std::string::String {
        self.from_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_from_id(&self) -> &str {
        match self.from_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_from_id_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.from_id
    }

    fn mut_from_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.from_id
    }

    // optional .Membership member = 4;

    pub fn clear_member(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_member(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::member(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_member(&mut self, v: Membership) {
        self.payload = ::std::option::Option::Some(Rumor_oneof_payload::member(v))
    }

    // Mutable pointer to the field.
    pub fn mut_member(&mut self) -> &mut Membership {
        if let ::std::option::Option::Some(Rumor_oneof_payload::member(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Rumor_oneof_payload::member(Membership::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::member(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_member(&mut self) -> Membership {
        if self.has_member() {
            match self.payload.take() {
                ::std::option::Option::Some(Rumor_oneof_payload::member(v)) => v,
                _ => panic!(),
            }
        } else {
            Membership::new()
        }
    }

    pub fn get_member(&self) -> &Membership {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::member(ref v)) => v,
            _ => Membership::default_instance(),
        }
    }

    // optional .Service service = 5;

    pub fn clear_service(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_service(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_service(&mut self, v: Service) {
        self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service(v))
    }

    // Mutable pointer to the field.
    pub fn mut_service(&mut self) -> &mut Service {
        if let ::std::option::Option::Some(Rumor_oneof_payload::service(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service(Service::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_service(&mut self) -> Service {
        if self.has_service() {
            match self.payload.take() {
                ::std::option::Option::Some(Rumor_oneof_payload::service(v)) => v,
                _ => panic!(),
            }
        } else {
            Service::new()
        }
    }

    pub fn get_service(&self) -> &Service {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service(ref v)) => v,
            _ => Service::default_instance(),
        }
    }

    // optional .ServiceConfig service_config = 6;

    pub fn clear_service_config(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_service_config(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service_config(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_service_config(&mut self, v: ServiceConfig) {
        self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service_config(v))
    }

    // Mutable pointer to the field.
    pub fn mut_service_config(&mut self) -> &mut ServiceConfig {
        if let ::std::option::Option::Some(Rumor_oneof_payload::service_config(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service_config(ServiceConfig::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service_config(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_service_config(&mut self) -> ServiceConfig {
        if self.has_service_config() {
            match self.payload.take() {
                ::std::option::Option::Some(Rumor_oneof_payload::service_config(v)) => v,
                _ => panic!(),
            }
        } else {
            ServiceConfig::new()
        }
    }

    pub fn get_service_config(&self) -> &ServiceConfig {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service_config(ref v)) => v,
            _ => ServiceConfig::default_instance(),
        }
    }

    // optional .ServiceFile service_file = 7;

    pub fn clear_service_file(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_service_file(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service_file(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_service_file(&mut self, v: ServiceFile) {
        self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service_file(v))
    }

    // Mutable pointer to the field.
    pub fn mut_service_file(&mut self) -> &mut ServiceFile {
        if let ::std::option::Option::Some(Rumor_oneof_payload::service_file(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service_file(ServiceFile::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service_file(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_service_file(&mut self) -> ServiceFile {
        if self.has_service_file() {
            match self.payload.take() {
                ::std::option::Option::Some(Rumor_oneof_payload::service_file(v)) => v,
                _ => panic!(),
            }
        } else {
            ServiceFile::new()
        }
    }

    pub fn get_service_file(&self) -> &ServiceFile {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::service_file(ref v)) => v,
            _ => ServiceFile::default_instance(),
        }
    }

    // optional .Election election = 8;

    pub fn clear_election(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_election(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::election(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_election(&mut self, v: Election) {
        self.payload = ::std::option::Option::Some(Rumor_oneof_payload::election(v))
    }

    // Mutable pointer to the field.
    pub fn mut_election(&mut self) -> &mut Election {
        if let ::std::option::Option::Some(Rumor_oneof_payload::election(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Rumor_oneof_payload::election(Election::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::election(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_election(&mut self) -> Election {
        if self.has_election() {
            match self.payload.take() {
                ::std::option::Option::Some(Rumor_oneof_payload::election(v)) => v,
                _ => panic!(),
            }
        } else {
            Election::new()
        }
    }

    pub fn get_election(&self) -> &Election {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::election(ref v)) => v,
            _ => Election::default_instance(),
        }
    }

    // optional .Departure departure = 9;

    pub fn clear_departure(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_departure(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::departure(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_departure(&mut self, v: Departure) {
        self.payload = ::std::option::Option::Some(Rumor_oneof_payload::departure(v))
    }

    // Mutable pointer to the field.
    pub fn mut_departure(&mut self) -> &mut Departure {
        if let ::std::option::Option::Some(Rumor_oneof_payload::departure(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Rumor_oneof_payload::departure(Departure::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::departure(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_departure(&mut self) -> Departure {
        if self.has_departure() {
            match self.payload.take() {
                ::std::option::Option::Some(Rumor_oneof_payload::departure(v)) => v,
                _ => panic!(),
            }
        } else {
            Departure::new()
        }
    }

    pub fn get_departure(&self) -> &Departure {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::departure(ref v)) => v,
            _ => Departure::default_instance(),
        }
    }

    // optional .Zone zone = 10;

    pub fn clear_zone(&mut self) {
        self.payload = ::std::option::Option::None;
    }

    pub fn has_zone(&self) -> bool {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::zone(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_zone(&mut self, v: Zone) {
        self.payload = ::std::option::Option::Some(Rumor_oneof_payload::zone(v))
    }

    // Mutable pointer to the field.
    pub fn mut_zone(&mut self) -> &mut Zone {
        if let ::std::option::Option::Some(Rumor_oneof_payload::zone(_)) = self.payload {
        } else {
            self.payload = ::std::option::Option::Some(Rumor_oneof_payload::zone(Zone::new()));
        }
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::zone(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_zone(&mut self) -> Zone {
        if self.has_zone() {
            match self.payload.take() {
                ::std::option::Option::Some(Rumor_oneof_payload::zone(v)) => v,
                _ => panic!(),
            }
        } else {
            Zone::new()
        }
    }

    pub fn get_zone(&self) -> &Zone {
        match self.payload {
            ::std::option::Option::Some(Rumor_oneof_payload::zone(ref v)) => v,
            _ => Zone::default_instance(),
        }
    }
}

impl ::protobuf::Message for Rumor {
    fn is_initialized(&self) -> bool {
        if self.field_type.is_none() {
            return false;
        }
        if let Some(Rumor_oneof_payload::member(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Rumor_oneof_payload::service(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Rumor_oneof_payload::service_config(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Rumor_oneof_payload::service_file(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Rumor_oneof_payload::election(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Rumor_oneof_payload::departure(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        if let Some(Rumor_oneof_payload::zone(ref v)) = self.payload {
            if !v.is_initialized() {
                return false;
            }
        }
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.field_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_repeated_string_into(wire_type, is, &mut self.tag)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.from_id)?;
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Rumor_oneof_payload::member(is.read_message()?));
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service(is.read_message()?));
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service_config(is.read_message()?));
                },
                7 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Rumor_oneof_payload::service_file(is.read_message()?));
                },
                8 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Rumor_oneof_payload::election(is.read_message()?));
                },
                9 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Rumor_oneof_payload::departure(is.read_message()?));
                },
                10 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    self.payload = ::std::option::Option::Some(Rumor_oneof_payload::zone(is.read_message()?));
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.field_type {
            my_size += ::protobuf::rt::enum_size(1, v);
        }
        for value in &self.tag {
            my_size += ::protobuf::rt::string_size(2, &value);
        };
        if let Some(ref v) = self.from_id.as_ref() {
            my_size += ::protobuf::rt::string_size(3, &v);
        }
        if let ::std::option::Option::Some(ref v) = self.payload {
            match v {
                &Rumor_oneof_payload::member(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Rumor_oneof_payload::service(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Rumor_oneof_payload::service_config(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Rumor_oneof_payload::service_file(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Rumor_oneof_payload::election(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Rumor_oneof_payload::departure(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &Rumor_oneof_payload::zone(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
            };
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.field_type {
            os.write_enum(1, v.value())?;
        }
        for v in &self.tag {
            os.write_string(2, &v)?;
        };
        if let Some(ref v) = self.from_id.as_ref() {
            os.write_string(3, &v)?;
        }
        if let ::std::option::Option::Some(ref v) = self.payload {
            match v {
                &Rumor_oneof_payload::member(ref v) => {
                    os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Rumor_oneof_payload::service(ref v) => {
                    os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Rumor_oneof_payload::service_config(ref v) => {
                    os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Rumor_oneof_payload::service_file(ref v) => {
                    os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Rumor_oneof_payload::election(ref v) => {
                    os.write_tag(8, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Rumor_oneof_payload::departure(ref v) => {
                    os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
                &Rumor_oneof_payload::zone(ref v) => {
                    os.write_tag(10, ::protobuf::wire_format::WireTypeLengthDelimited)?;
                    os.write_raw_varint32(v.get_cached_size())?;
                    v.write_to_with_cached_sizes(os)?;
                },
            };
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Rumor {
    fn new() -> Rumor {
        Rumor::new()
    }

    fn descriptor_static(_: ::std::option::Option<Rumor>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<Rumor_Type>>(
                    "type",
                    Rumor::get_field_type_for_reflect,
                    Rumor::mut_field_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "tag",
                    Rumor::get_tag_for_reflect,
                    Rumor::mut_tag_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "from_id",
                    Rumor::get_from_id_for_reflect,
                    Rumor::mut_from_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, Membership>(
                    "member",
                    Rumor::has_member,
                    Rumor::get_member,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, Service>(
                    "service",
                    Rumor::has_service,
                    Rumor::get_service,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, ServiceConfig>(
                    "service_config",
                    Rumor::has_service_config,
                    Rumor::get_service_config,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, ServiceFile>(
                    "service_file",
                    Rumor::has_service_file,
                    Rumor::get_service_file,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, Election>(
                    "election",
                    Rumor::has_election,
                    Rumor::get_election,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, Departure>(
                    "departure",
                    Rumor::has_departure,
                    Rumor::get_departure,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor::<_, Zone>(
                    "zone",
                    Rumor::has_zone,
                    Rumor::get_zone,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Rumor>(
                    "Rumor",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Rumor {
    fn clear(&mut self) {
        self.clear_field_type();
        self.clear_tag();
        self.clear_from_id();
        self.clear_member();
        self.clear_service();
        self.clear_service_config();
        self.clear_service_file();
        self.clear_election();
        self.clear_departure();
        self.clear_zone();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Rumor {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Rumor {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum Rumor_Type {
    Member = 1,
    Service = 2,
    Election = 3,
    ServiceConfig = 4,
    ServiceFile = 5,
    Fake = 6,
    Fake2 = 7,
    ElectionUpdate = 8,
    Departure = 9,
    Zone = 10,
}

impl ::protobuf::ProtobufEnum for Rumor_Type {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<Rumor_Type> {
        match value {
            1 => ::std::option::Option::Some(Rumor_Type::Member),
            2 => ::std::option::Option::Some(Rumor_Type::Service),
            3 => ::std::option::Option::Some(Rumor_Type::Election),
            4 => ::std::option::Option::Some(Rumor_Type::ServiceConfig),
            5 => ::std::option::Option::Some(Rumor_Type::ServiceFile),
            6 => ::std::option::Option::Some(Rumor_Type::Fake),
            7 => ::std::option::Option::Some(Rumor_Type::Fake2),
            8 => ::std::option::Option::Some(Rumor_Type::ElectionUpdate),
            9 => ::std::option::Option::Some(Rumor_Type::Departure),
            10 => ::std::option::Option::Some(Rumor_Type::Zone),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [Rumor_Type] = &[
            Rumor_Type::Member,
            Rumor_Type::Service,
            Rumor_Type::Election,
            Rumor_Type::ServiceConfig,
            Rumor_Type::ServiceFile,
            Rumor_Type::Fake,
            Rumor_Type::Fake2,
            Rumor_Type::ElectionUpdate,
            Rumor_Type::Departure,
            Rumor_Type::Zone,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<Rumor_Type>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("Rumor_Type", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for Rumor_Type {
}

impl ::protobuf::reflect::ProtobufValue for Rumor_Type {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Wire {
    // message fields
    encrypted: ::std::option::Option<bool>,
    nonce: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    payload: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Wire {}

impl Wire {
    pub fn new() -> Wire {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Wire {
        static mut instance: ::protobuf::lazy::Lazy<Wire> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Wire,
        };
        unsafe {
            instance.get(Wire::new)
        }
    }

    // optional bool encrypted = 1;

    pub fn clear_encrypted(&mut self) {
        self.encrypted = ::std::option::Option::None;
    }

    pub fn has_encrypted(&self) -> bool {
        self.encrypted.is_some()
    }

    // Param is passed by value, moved
    pub fn set_encrypted(&mut self, v: bool) {
        self.encrypted = ::std::option::Option::Some(v);
    }

    pub fn get_encrypted(&self) -> bool {
        self.encrypted.unwrap_or(false)
    }

    fn get_encrypted_for_reflect(&self) -> &::std::option::Option<bool> {
        &self.encrypted
    }

    fn mut_encrypted_for_reflect(&mut self) -> &mut ::std::option::Option<bool> {
        &mut self.encrypted
    }

    // optional bytes nonce = 2;

    pub fn clear_nonce(&mut self) {
        self.nonce.clear();
    }

    pub fn has_nonce(&self) -> bool {
        self.nonce.is_some()
    }

    // Param is passed by value, moved
    pub fn set_nonce(&mut self, v: ::std::vec::Vec<u8>) {
        self.nonce = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_nonce(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.nonce.is_none() {
            self.nonce.set_default();
        }
        self.nonce.as_mut().unwrap()
    }

    // Take field
    pub fn take_nonce(&mut self) -> ::std::vec::Vec<u8> {
        self.nonce.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_nonce(&self) -> &[u8] {
        match self.nonce.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_nonce_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.nonce
    }

    fn mut_nonce_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.nonce
    }

    // optional bytes payload = 3;

    pub fn clear_payload(&mut self) {
        self.payload.clear();
    }

    pub fn has_payload(&self) -> bool {
        self.payload.is_some()
    }

    // Param is passed by value, moved
    pub fn set_payload(&mut self, v: ::std::vec::Vec<u8>) {
        self.payload = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_payload(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.payload.is_none() {
            self.payload.set_default();
        }
        self.payload.as_mut().unwrap()
    }

    // Take field
    pub fn take_payload(&mut self) -> ::std::vec::Vec<u8> {
        self.payload.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_payload(&self) -> &[u8] {
        match self.payload.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_payload_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.payload
    }

    fn mut_payload_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.payload
    }
}

impl ::protobuf::Message for Wire {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_bool()?;
                    self.encrypted = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.nonce)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.payload)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.encrypted {
            my_size += 2;
        }
        if let Some(ref v) = self.nonce.as_ref() {
            my_size += ::protobuf::rt::bytes_size(2, &v);
        }
        if let Some(ref v) = self.payload.as_ref() {
            my_size += ::protobuf::rt::bytes_size(3, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.encrypted {
            os.write_bool(1, v)?;
        }
        if let Some(ref v) = self.nonce.as_ref() {
            os.write_bytes(2, &v)?;
        }
        if let Some(ref v) = self.payload.as_ref() {
            os.write_bytes(3, &v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Wire {
    fn new() -> Wire {
        Wire::new()
    }

    fn descriptor_static(_: ::std::option::Option<Wire>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeBool>(
                    "encrypted",
                    Wire::get_encrypted_for_reflect,
                    Wire::mut_encrypted_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "nonce",
                    Wire::get_nonce_for_reflect,
                    Wire::mut_nonce_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "payload",
                    Wire::get_payload_for_reflect,
                    Wire::mut_payload_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Wire>(
                    "Wire",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Wire {
    fn clear(&mut self) {
        self.clear_encrypted();
        self.clear_nonce();
        self.clear_payload();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Wire {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x14protocols/swim.proto\"~\n\x0bZoneAddress\x12\x17\n\x07zone_id\x18\
    \x01\x20\x01(\tR\x06zoneId\x12\x18\n\x07address\x18\x02\x20\x01(\tR\x07a\
    ddress\x12\x1b\n\tswim_port\x18\x03\x20\x01(\x05R\x08swimPort\x12\x1f\n\
    \x0bgossip_port\x18\x04\x20\x01(\x05R\ngossipPort\"\xb6\x02\n\x06Member\
    \x12\x0e\n\x02id\x18\x01\x20\x01(\tR\x02id\x12\x20\n\x0bincarnation\x18\
    \x02\x20\x01(\x04R\x0bincarnation\x12\x18\n\x07address\x18\x03\x20\x01(\
    \tR\x07address\x12\x1b\n\tswim_port\x18\x04\x20\x01(\x05R\x08swimPort\
    \x12\x1f\n\x0bgossip_port\x18\x05\x20\x01(\x05R\ngossipPort\x12%\n\npers\
    istent\x18\x06\x20\x01(\x08:\x05falseR\npersistent\x12!\n\x08departed\
    \x18\x07\x20\x01(\x08:\x05falseR\x08departed\x12\x17\n\x07zone_id\x18\
    \x08\x20\x01(\tR\x06zoneId\x12?\n\x14additional_addresses\x18\t\x20\x03(\
    \x0b2\x0c.ZoneAddressR\x13additionalAddresses\"\xeb\x01\n\x04Zone\x12\
    \x0e\n\x02id\x18\x01\x20\x01(\tR\x02id\x12\x20\n\x0bincarnation\x18\x02\
    \x20\x01(\x04R\x0bincarnation\x12#\n\rmaintainer_id\x18\x03\x20\x01(\tR\
    \x0cmaintainerId\x12$\n\x0eparent_zone_id\x18\x04\x20\x01(\tR\x0cparentZ\
    oneId\x12$\n\x0echild_zone_ids\x18\x05\x20\x03(\tR\x0cchildZoneIds\x12\
    \x1c\n\tsuccessor\x18\x06\x20\x01(\tR\tsuccessor\x12\"\n\x0cpredecessors\
    \x18\x07\x20\x03(\tR\x0cpredecessors\"d\n\x04Ping\x12\x1b\n\x04from\x18\
    \x01\x20\x01(\x0b2\x07.MemberR\x04from\x12&\n\nforward_to\x18\x02\x20\
    \x01(\x0b2\x07.MemberR\tforwardTo\x12\x17\n\x02to\x18\x03\x20\x01(\x0b2\
    \x07.MemberR\x02to\"c\n\x03Ack\x12\x1b\n\x04from\x18\x01\x20\x01(\x0b2\
    \x07.MemberR\x04from\x12&\n\nforward_to\x18\x02\x20\x01(\x0b2\x07.Member\
    R\tforwardTo\x12\x17\n\x02to\x18\x03\x20\x01(\x0b2\x07.MemberR\x02to\"`\
    \n\x07PingReq\x12\x1b\n\x04from\x18\x01\x20\x01(\x0b2\x07.MemberR\x04fro\
    m\x12\x1f\n\x06target\x18\x02\x20\x01(\x0b2\x07.MemberR\x06target\x12\
    \x17\n\x02to\x18\x03\x20\x01(\x0b2\x07.MemberR\x02to\"\xcd\x01\n\nZoneCh\
    ange\x12\x1b\n\x04from\x18\x01\x20\x01(\x0b2\x07.MemberR\x04from\x12\x17\
    \n\x07zone_id\x18\x02\x20\x01(\tR\x06zoneId\x12*\n\rnew_successor\x18\
    \x03\x20\x01(\x0b2\x05.ZoneR\x0cnewSuccessor\x120\n\x10new_predecessors\
    \x18\x04\x20\x03(\x0b2\x05.ZoneR\x0fnewPredecessors\x12+\n\x11drop_prede\
    cessors\x18\x05\x20\x03(\tR\x10dropPredecessors\"\x98\x01\n\nMembership\
    \x12\x1f\n\x06member\x18\x01\x20\x01(\x0b2\x07.MemberR\x06member\x12*\n\
    \x06health\x18\x02\x20\x01(\x0e2\x12.Membership.HealthR\x06health\"=\n\
    \x06Health\x12\t\n\x05ALIVE\x10\x01\x12\x0b\n\x07SUSPECT\x10\x02\x12\r\n\
    \tCONFIRMED\x10\x03\x12\x0c\n\x08DEPARTED\x10\x04\"\xf5\x01\n\x08Electio\
    n\x12\x1b\n\tmember_id\x18\x01\x20\x01(\tR\x08memberId\x12#\n\rservice_g\
    roup\x18\x02\x20\x01(\tR\x0cserviceGroup\x12\x12\n\x04term\x18\x03\x20\
    \x01(\x04R\x04term\x12\x20\n\x0bsuitability\x18\x04\x20\x01(\x04R\x0bsui\
    tability\x12(\n\x06status\x18\x05\x20\x01(\x0e2\x10.Election.StatusR\x06\
    status\x12\x14\n\x05votes\x18\x06\x20\x03(\tR\x05votes\"1\n\x06Status\
    \x12\x0b\n\x07Running\x10\x01\x12\x0c\n\x08NoQuorum\x10\x02\x12\x0c\n\
    \x08Finished\x10\x03\"\xcf\x01\n\x07Service\x12\x1b\n\tmember_id\x18\x01\
    \x20\x01(\tR\x08memberId\x12#\n\rservice_group\x18\x02\x20\x01(\tR\x0cse\
    rviceGroup\x12\x20\n\x0bincarnation\x18\x03\x20\x01(\x04R\x0bincarnation\
    \x12\x20\n\x0binitialized\x18\x08\x20\x01(\x08R\x0binitialized\x12\x10\n\
    \x03pkg\x18\t\x20\x01(\tR\x03pkg\x12\x10\n\x03cfg\x18\n\x20\x01(\x0cR\
    \x03cfg\x12\x1a\n\x03sys\x18\x0c\x20\x01(\x0b2\x08.SysInfoR\x03sys\"\x8c\
    \x01\n\rServiceConfig\x12#\n\rservice_group\x18\x01\x20\x01(\tR\x0cservi\
    ceGroup\x12\x20\n\x0bincarnation\x18\x02\x20\x01(\x04R\x0bincarnation\
    \x12\x1c\n\tencrypted\x18\x03\x20\x01(\x08R\tencrypted\x12\x16\n\x06conf\
    ig\x18\x04\x20\x01(\x0cR\x06config\"\xa2\x01\n\x0bServiceFile\x12#\n\rse\
    rvice_group\x18\x01\x20\x01(\tR\x0cserviceGroup\x12\x20\n\x0bincarnation\
    \x18\x02\x20\x01(\x04R\x0bincarnation\x12\x1c\n\tencrypted\x18\x03\x20\
    \x01(\x08R\tencrypted\x12\x1a\n\x08filename\x18\x04\x20\x01(\tR\x08filen\
    ame\x12\x12\n\x04body\x18\x05\x20\x01(\x0cR\x04body\"\x95\x03\n\x07SysIn\
    fo\x12\x19\n\x02ip\x18\x01\x20\x01(\t:\t127.0.0.1R\x02ip\x12%\n\x08hostn\
    ame\x18\x02\x20\x01(\t:\tlocalhostR\x08hostname\x12&\n\tgossip_ip\x18\
    \x03\x20\x01(\t:\t127.0.0.1R\x08gossipIp\x12\x1f\n\x0bgossip_port\x18\
    \x04\x20\x01(\rR\ngossipPort\x121\n\x0fhttp_gateway_ip\x18\x05\x20\x01(\
    \t:\t127.0.0.1R\rhttpGatewayIp\x12*\n\x11http_gateway_port\x18\x06\x20\
    \x01(\rR\x0fhttpGatewayPort\x12/\n\x0ectl_gateway_ip\x18\x07\x20\x01(\t:\
    \t127.0.0.1R\x0cctlGatewayIp\x12.\n\x10ctl_gateway_port\x18\x08\x20\x01(\
    \r:\x049632R\x0ectlGatewayPort\x12?\n\x14additional_addresses\x18\t\x20\
    \x03(\x0b2\x0c.ZoneAddressR\x13additionalAddresses\"(\n\tDeparture\x12\
    \x1b\n\tmember_id\x18\x01\x20\x01(\tR\x08memberId\"\xc1\x02\n\x04Swim\
    \x12\x1e\n\x04type\x18\x01\x20\x02(\x0e2\n.Swim.TypeR\x04type\x12\x1b\n\
    \x04ping\x18\x02\x20\x01(\x0b2\x05.PingH\0R\x04ping\x12\x18\n\x03ack\x18\
    \x03\x20\x01(\x0b2\x04.AckH\0R\x03ack\x12$\n\x07pingreq\x18\x04\x20\x01(\
    \x0b2\x08.PingReqH\0R\x07pingreq\x12.\n\x0bzone_change\x18\x07\x20\x01(\
    \x0b2\x0b.ZoneChangeH\0R\nzoneChange\x12+\n\nmembership\x18\x05\x20\x03(\
    \x0b2\x0b.MembershipR\nmembership\x12\x1b\n\x05zones\x18\x06\x20\x03(\
    \x0b2\x05.ZoneR\x05zones\"7\n\x04Type\x12\x08\n\x04PING\x10\x01\x12\x07\
    \n\x03ACK\x10\x02\x12\x0b\n\x07PINGREQ\x10\x03\x12\x0f\n\x0bZONE_CHANGE\
    \x10\x04B\t\n\x07payload\"\x9f\x04\n\x05Rumor\x12\x1f\n\x04type\x18\x01\
    \x20\x02(\x0e2\x0b.Rumor.TypeR\x04type\x12\x10\n\x03tag\x18\x02\x20\x03(\
    \tR\x03tag\x12\x17\n\x07from_id\x18\x03\x20\x01(\tR\x06fromId\x12%\n\x06\
    member\x18\x04\x20\x01(\x0b2\x0b.MembershipH\0R\x06member\x12$\n\x07serv\
    ice\x18\x05\x20\x01(\x0b2\x08.ServiceH\0R\x07service\x127\n\x0eservice_c\
    onfig\x18\x06\x20\x01(\x0b2\x0e.ServiceConfigH\0R\rserviceConfig\x121\n\
    \x0cservice_file\x18\x07\x20\x01(\x0b2\x0c.ServiceFileH\0R\x0bserviceFil\
    e\x12'\n\x08election\x18\x08\x20\x01(\x0b2\t.ElectionH\0R\x08election\
    \x12*\n\tdeparture\x18\t\x20\x01(\x0b2\n.DepartureH\0R\tdeparture\x12\
    \x1b\n\x04zone\x18\n\x20\x01(\x0b2\x05.ZoneH\0R\x04zone\"\x93\x01\n\x04T\
    ype\x12\n\n\x06Member\x10\x01\x12\x0b\n\x07Service\x10\x02\x12\x0c\n\x08\
    Election\x10\x03\x12\x11\n\rServiceConfig\x10\x04\x12\x0f\n\x0bServiceFi\
    le\x10\x05\x12\x08\n\x04Fake\x10\x06\x12\t\n\x05Fake2\x10\x07\x12\x12\n\
    \x0eElectionUpdate\x10\x08\x12\r\n\tDeparture\x10\t\x12\x08\n\x04Zone\
    \x10\nB\t\n\x07payload\"T\n\x04Wire\x12\x1c\n\tencrypted\x18\x01\x20\x01\
    (\x08R\tencrypted\x12\x14\n\x05nonce\x18\x02\x20\x01(\x0cR\x05nonce\x12\
    \x18\n\x07payload\x18\x03\x20\x01(\x0cR\x07payloadJ\xbb=\n\x07\x12\x05\0\
    \0\xa3\x01\x01\n\x08\n\x01\x0c\x12\x03\0\0\x12\n\n\n\x02\x04\0\x12\x04\
    \x02\0\x07\x01\n\n\n\x03\x04\0\x01\x12\x03\x02\x08\x13\n\x0b\n\x04\x04\0\
    \x02\0\x12\x03\x03\x02\x1e\n\x0c\n\x05\x04\0\x02\0\x04\x12\x03\x03\x02\n\
    \n\x0c\n\x05\x04\0\x02\0\x05\x12\x03\x03\x0b\x11\n\x0c\n\x05\x04\0\x02\0\
    \x01\x12\x03\x03\x12\x19\n\x0c\n\x05\x04\0\x02\0\x03\x12\x03\x03\x1c\x1d\
    \n\x0b\n\x04\x04\0\x02\x01\x12\x03\x04\x02\x1e\n\x0c\n\x05\x04\0\x02\x01\
    \x04\x12\x03\x04\x02\n\n\x0c\n\x05\x04\0\x02\x01\x05\x12\x03\x04\x0b\x11\
    \n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x04\x12\x19\n\x0c\n\x05\x04\0\x02\
    \x01\x03\x12\x03\x04\x1c\x1d\n\x0b\n\x04\x04\0\x02\x02\x12\x03\x05\x02\
    \x1f\n\x0c\n\x05\x04\0\x02\x02\x04\x12\x03\x05\x02\n\n\x0c\n\x05\x04\0\
    \x02\x02\x05\x12\x03\x05\x0b\x10\n\x0c\n\x05\x04\0\x02\x02\x01\x12\x03\
    \x05\x11\x1a\n\x0c\n\x05\x04\0\x02\x02\x03\x12\x03\x05\x1d\x1e\n\x0b\n\
    \x04\x04\0\x02\x03\x12\x03\x06\x02!\n\x0c\n\x05\x04\0\x02\x03\x04\x12\
    \x03\x06\x02\n\n\x0c\n\x05\x04\0\x02\x03\x05\x12\x03\x06\x0b\x10\n\x0c\n\
    \x05\x04\0\x02\x03\x01\x12\x03\x06\x11\x1c\n\x0c\n\x05\x04\0\x02\x03\x03\
    \x12\x03\x06\x1f\x20\n\n\n\x02\x04\x01\x12\x04\t\0\x13\x01\n\n\n\x03\x04\
    \x01\x01\x12\x03\t\x08\x0e\n\x0b\n\x04\x04\x01\x02\0\x12\x03\n\x02\x19\n\
    \x0c\n\x05\x04\x01\x02\0\x04\x12\x03\n\x02\n\n\x0c\n\x05\x04\x01\x02\0\
    \x05\x12\x03\n\x0b\x11\n\x0c\n\x05\x04\x01\x02\0\x01\x12\x03\n\x12\x14\n\
    \x0c\n\x05\x04\x01\x02\0\x03\x12\x03\n\x17\x18\n\x0b\n\x04\x04\x01\x02\
    \x01\x12\x03\x0b\x02\"\n\x0c\n\x05\x04\x01\x02\x01\x04\x12\x03\x0b\x02\n\
    \n\x0c\n\x05\x04\x01\x02\x01\x05\x12\x03\x0b\x0b\x11\n\x0c\n\x05\x04\x01\
    \x02\x01\x01\x12\x03\x0b\x12\x1d\n\x0c\n\x05\x04\x01\x02\x01\x03\x12\x03\
    \x0b\x20!\n\x0b\n\x04\x04\x01\x02\x02\x12\x03\x0c\x02\x1e\n\x0c\n\x05\
    \x04\x01\x02\x02\x04\x12\x03\x0c\x02\n\n\x0c\n\x05\x04\x01\x02\x02\x05\
    \x12\x03\x0c\x0b\x11\n\x0c\n\x05\x04\x01\x02\x02\x01\x12\x03\x0c\x12\x19\
    \n\x0c\n\x05\x04\x01\x02\x02\x03\x12\x03\x0c\x1c\x1d\n\x0b\n\x04\x04\x01\
    \x02\x03\x12\x03\r\x02\x1f\n\x0c\n\x05\x04\x01\x02\x03\x04\x12\x03\r\x02\
    \n\n\x0c\n\x05\x04\x01\x02\x03\x05\x12\x03\r\x0b\x10\n\x0c\n\x05\x04\x01\
    \x02\x03\x01\x12\x03\r\x11\x1a\n\x0c\n\x05\x04\x01\x02\x03\x03\x12\x03\r\
    \x1d\x1e\n\x0b\n\x04\x04\x01\x02\x04\x12\x03\x0e\x02!\n\x0c\n\x05\x04\
    \x01\x02\x04\x04\x12\x03\x0e\x02\n\n\x0c\n\x05\x04\x01\x02\x04\x05\x12\
    \x03\x0e\x0b\x10\n\x0c\n\x05\x04\x01\x02\x04\x01\x12\x03\x0e\x11\x1c\n\
    \x0c\n\x05\x04\x01\x02\x04\x03\x12\x03\x0e\x1f\x20\n\x0b\n\x04\x04\x01\
    \x02\x05\x12\x03\x0f\x021\n\x0c\n\x05\x04\x01\x02\x05\x04\x12\x03\x0f\
    \x02\n\n\x0c\n\x05\x04\x01\x02\x05\x05\x12\x03\x0f\x0b\x0f\n\x0c\n\x05\
    \x04\x01\x02\x05\x01\x12\x03\x0f\x10\x1a\n\x0c\n\x05\x04\x01\x02\x05\x03\
    \x12\x03\x0f\x1d\x1e\n\x0c\n\x05\x04\x01\x02\x05\x08\x12\x03\x0f\x1f0\n\
    \x0c\n\x05\x04\x01\x02\x05\x07\x12\x03\x0f*/\n\x0b\n\x04\x04\x01\x02\x06\
    \x12\x03\x10\x02/\n\x0c\n\x05\x04\x01\x02\x06\x04\x12\x03\x10\x02\n\n\
    \x0c\n\x05\x04\x01\x02\x06\x05\x12\x03\x10\x0b\x0f\n\x0c\n\x05\x04\x01\
    \x02\x06\x01\x12\x03\x10\x10\x18\n\x0c\n\x05\x04\x01\x02\x06\x03\x12\x03\
    \x10\x1b\x1c\n\x0c\n\x05\x04\x01\x02\x06\x08\x12\x03\x10\x1d.\n\x0c\n\
    \x05\x04\x01\x02\x06\x07\x12\x03\x10(-\n\x0b\n\x04\x04\x01\x02\x07\x12\
    \x03\x11\x02\x1e\n\x0c\n\x05\x04\x01\x02\x07\x04\x12\x03\x11\x02\n\n\x0c\
    \n\x05\x04\x01\x02\x07\x05\x12\x03\x11\x0b\x11\n\x0c\n\x05\x04\x01\x02\
    \x07\x01\x12\x03\x11\x12\x19\n\x0c\n\x05\x04\x01\x02\x07\x03\x12\x03\x11\
    \x1c\x1d\n\x0b\n\x04\x04\x01\x02\x08\x12\x03\x12\x020\n\x0c\n\x05\x04\
    \x01\x02\x08\x04\x12\x03\x12\x02\n\n\x0c\n\x05\x04\x01\x02\x08\x06\x12\
    \x03\x12\x0b\x16\n\x0c\n\x05\x04\x01\x02\x08\x01\x12\x03\x12\x17+\n\x0c\
    \n\x05\x04\x01\x02\x08\x03\x12\x03\x12./\n\n\n\x02\x04\x02\x12\x04\x15\0\
    \x1d\x01\n\n\n\x03\x04\x02\x01\x12\x03\x15\x08\x0c\n\x0b\n\x04\x04\x02\
    \x02\0\x12\x03\x16\x02\x19\n\x0c\n\x05\x04\x02\x02\0\x04\x12\x03\x16\x02\
    \n\n\x0c\n\x05\x04\x02\x02\0\x05\x12\x03\x16\x0b\x11\n\x0c\n\x05\x04\x02\
    \x02\0\x01\x12\x03\x16\x12\x14\n\x0c\n\x05\x04\x02\x02\0\x03\x12\x03\x16\
    \x17\x18\n\x0b\n\x04\x04\x02\x02\x01\x12\x03\x17\x02\"\n\x0c\n\x05\x04\
    \x02\x02\x01\x04\x12\x03\x17\x02\n\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\
    \x03\x17\x0b\x11\n\x0c\n\x05\x04\x02\x02\x01\x01\x12\x03\x17\x12\x1d\n\
    \x0c\n\x05\x04\x02\x02\x01\x03\x12\x03\x17\x20!\n\x0b\n\x04\x04\x02\x02\
    \x02\x12\x03\x18\x02$\n\x0c\n\x05\x04\x02\x02\x02\x04\x12\x03\x18\x02\n\
    \n\x0c\n\x05\x04\x02\x02\x02\x05\x12\x03\x18\x0b\x11\n\x0c\n\x05\x04\x02\
    \x02\x02\x01\x12\x03\x18\x12\x1f\n\x0c\n\x05\x04\x02\x02\x02\x03\x12\x03\
    \x18\"#\n\x0b\n\x04\x04\x02\x02\x03\x12\x03\x19\x02%\n\x0c\n\x05\x04\x02\
    \x02\x03\x04\x12\x03\x19\x02\n\n\x0c\n\x05\x04\x02\x02\x03\x05\x12\x03\
    \x19\x0b\x11\n\x0c\n\x05\x04\x02\x02\x03\x01\x12\x03\x19\x12\x20\n\x0c\n\
    \x05\x04\x02\x02\x03\x03\x12\x03\x19#$\n\x0b\n\x04\x04\x02\x02\x04\x12\
    \x03\x1a\x02%\n\x0c\n\x05\x04\x02\x02\x04\x04\x12\x03\x1a\x02\n\n\x0c\n\
    \x05\x04\x02\x02\x04\x05\x12\x03\x1a\x0b\x11\n\x0c\n\x05\x04\x02\x02\x04\
    \x01\x12\x03\x1a\x12\x20\n\x0c\n\x05\x04\x02\x02\x04\x03\x12\x03\x1a#$\n\
    \x0b\n\x04\x04\x02\x02\x05\x12\x03\x1b\x02\x20\n\x0c\n\x05\x04\x02\x02\
    \x05\x04\x12\x03\x1b\x02\n\n\x0c\n\x05\x04\x02\x02\x05\x05\x12\x03\x1b\
    \x0b\x11\n\x0c\n\x05\x04\x02\x02\x05\x01\x12\x03\x1b\x12\x1b\n\x0c\n\x05\
    \x04\x02\x02\x05\x03\x12\x03\x1b\x1e\x1f\n\x0b\n\x04\x04\x02\x02\x06\x12\
    \x03\x1c\x02#\n\x0c\n\x05\x04\x02\x02\x06\x04\x12\x03\x1c\x02\n\n\x0c\n\
    \x05\x04\x02\x02\x06\x05\x12\x03\x1c\x0b\x11\n\x0c\n\x05\x04\x02\x02\x06\
    \x01\x12\x03\x1c\x12\x1e\n\x0c\n\x05\x04\x02\x02\x06\x03\x12\x03\x1c!\"\
    \n\n\n\x02\x04\x03\x12\x04\x1f\0#\x01\n\n\n\x03\x04\x03\x01\x12\x03\x1f\
    \x08\x0c\n\x0b\n\x04\x04\x03\x02\0\x12\x03\x20\x02\x1b\n\x0c\n\x05\x04\
    \x03\x02\0\x04\x12\x03\x20\x02\n\n\x0c\n\x05\x04\x03\x02\0\x06\x12\x03\
    \x20\x0b\x11\n\x0c\n\x05\x04\x03\x02\0\x01\x12\x03\x20\x12\x16\n\x0c\n\
    \x05\x04\x03\x02\0\x03\x12\x03\x20\x19\x1a\n\x0b\n\x04\x04\x03\x02\x01\
    \x12\x03!\x02!\n\x0c\n\x05\x04\x03\x02\x01\x04\x12\x03!\x02\n\n\x0c\n\
    \x05\x04\x03\x02\x01\x06\x12\x03!\x0b\x11\n\x0c\n\x05\x04\x03\x02\x01\
    \x01\x12\x03!\x12\x1c\n\x0c\n\x05\x04\x03\x02\x01\x03\x12\x03!\x1f\x20\n\
    \x0b\n\x04\x04\x03\x02\x02\x12\x03\"\x02\x19\n\x0c\n\x05\x04\x03\x02\x02\
    \x04\x12\x03\"\x02\n\n\x0c\n\x05\x04\x03\x02\x02\x06\x12\x03\"\x0b\x11\n\
    \x0c\n\x05\x04\x03\x02\x02\x01\x12\x03\"\x12\x14\n\x0c\n\x05\x04\x03\x02\
    \x02\x03\x12\x03\"\x17\x18\n\n\n\x02\x04\x04\x12\x04%\0)\x01\n\n\n\x03\
    \x04\x04\x01\x12\x03%\x08\x0b\n\x0b\n\x04\x04\x04\x02\0\x12\x03&\x02\x1b\
    \n\x0c\n\x05\x04\x04\x02\0\x04\x12\x03&\x02\n\n\x0c\n\x05\x04\x04\x02\0\
    \x06\x12\x03&\x0b\x11\n\x0c\n\x05\x04\x04\x02\0\x01\x12\x03&\x12\x16\n\
    \x0c\n\x05\x04\x04\x02\0\x03\x12\x03&\x19\x1a\n\x0b\n\x04\x04\x04\x02\
    \x01\x12\x03'\x02!\n\x0c\n\x05\x04\x04\x02\x01\x04\x12\x03'\x02\n\n\x0c\
    \n\x05\x04\x04\x02\x01\x06\x12\x03'\x0b\x11\n\x0c\n\x05\x04\x04\x02\x01\
    \x01\x12\x03'\x12\x1c\n\x0c\n\x05\x04\x04\x02\x01\x03\x12\x03'\x1f\x20\n\
    \x0b\n\x04\x04\x04\x02\x02\x12\x03(\x02\x19\n\x0c\n\x05\x04\x04\x02\x02\
    \x04\x12\x03(\x02\n\n\x0c\n\x05\x04\x04\x02\x02\x06\x12\x03(\x0b\x11\n\
    \x0c\n\x05\x04\x04\x02\x02\x01\x12\x03(\x12\x14\n\x0c\n\x05\x04\x04\x02\
    \x02\x03\x12\x03(\x17\x18\n\n\n\x02\x04\x05\x12\x04+\0/\x01\n\n\n\x03\
    \x04\x05\x01\x12\x03+\x08\x0f\n\x0b\n\x04\x04\x05\x02\0\x12\x03,\x02\x1b\
    \n\x0c\n\x05\x04\x05\x02\0\x04\x12\x03,\x02\n\n\x0c\n\x05\x04\x05\x02\0\
    \x06\x12\x03,\x0b\x11\n\x0c\n\x05\x04\x05\x02\0\x01\x12\x03,\x12\x16\n\
    \x0c\n\x05\x04\x05\x02\0\x03\x12\x03,\x19\x1a\n\x0b\n\x04\x04\x05\x02\
    \x01\x12\x03-\x02\x1d\n\x0c\n\x05\x04\x05\x02\x01\x04\x12\x03-\x02\n\n\
    \x0c\n\x05\x04\x05\x02\x01\x06\x12\x03-\x0b\x11\n\x0c\n\x05\x04\x05\x02\
    \x01\x01\x12\x03-\x12\x18\n\x0c\n\x05\x04\x05\x02\x01\x03\x12\x03-\x1b\
    \x1c\n\x0b\n\x04\x04\x05\x02\x02\x12\x03.\x02\x19\n\x0c\n\x05\x04\x05\
    \x02\x02\x04\x12\x03.\x02\n\n\x0c\n\x05\x04\x05\x02\x02\x06\x12\x03.\x0b\
    \x11\n\x0c\n\x05\x04\x05\x02\x02\x01\x12\x03.\x12\x14\n\x0c\n\x05\x04\
    \x05\x02\x02\x03\x12\x03.\x17\x18\n\n\n\x02\x04\x06\x12\x041\07\x01\n\n\
    \n\x03\x04\x06\x01\x12\x031\x08\x12\n\x0b\n\x04\x04\x06\x02\0\x12\x032\
    \x02\x1b\n\x0c\n\x05\x04\x06\x02\0\x04\x12\x032\x02\n\n\x0c\n\x05\x04\
    \x06\x02\0\x06\x12\x032\x0b\x11\n\x0c\n\x05\x04\x06\x02\0\x01\x12\x032\
    \x12\x16\n\x0c\n\x05\x04\x06\x02\0\x03\x12\x032\x19\x1a\n\x0b\n\x04\x04\
    \x06\x02\x01\x12\x033\x02\x1e\n\x0c\n\x05\x04\x06\x02\x01\x04\x12\x033\
    \x02\n\n\x0c\n\x05\x04\x06\x02\x01\x05\x12\x033\x0b\x11\n\x0c\n\x05\x04\
    \x06\x02\x01\x01\x12\x033\x12\x19\n\x0c\n\x05\x04\x06\x02\x01\x03\x12\
    \x033\x1c\x1d\n\x0b\n\x04\x04\x06\x02\x02\x12\x034\x02\"\n\x0c\n\x05\x04\
    \x06\x02\x02\x04\x12\x034\x02\n\n\x0c\n\x05\x04\x06\x02\x02\x06\x12\x034\
    \x0b\x0f\n\x0c\n\x05\x04\x06\x02\x02\x01\x12\x034\x10\x1d\n\x0c\n\x05\
    \x04\x06\x02\x02\x03\x12\x034\x20!\n\x0b\n\x04\x04\x06\x02\x03\x12\x035\
    \x02%\n\x0c\n\x05\x04\x06\x02\x03\x04\x12\x035\x02\n\n\x0c\n\x05\x04\x06\
    \x02\x03\x06\x12\x035\x0b\x0f\n\x0c\n\x05\x04\x06\x02\x03\x01\x12\x035\
    \x10\x20\n\x0c\n\x05\x04\x06\x02\x03\x03\x12\x035#$\n\x0b\n\x04\x04\x06\
    \x02\x04\x12\x036\x02(\n\x0c\n\x05\x04\x06\x02\x04\x04\x12\x036\x02\n\n\
    \x0c\n\x05\x04\x06\x02\x04\x05\x12\x036\x0b\x11\n\x0c\n\x05\x04\x06\x02\
    \x04\x01\x12\x036\x12#\n\x0c\n\x05\x04\x06\x02\x04\x03\x12\x036&'\n\n\n\
    \x02\x04\x07\x12\x049\0>\x01\n\n\n\x03\x04\x07\x01\x12\x039\x08\x12\n\
    \x0b\n\x04\x04\x07\x04\0\x12\x03:\x02F\n\x0c\n\x05\x04\x07\x04\0\x01\x12\
    \x03:\x07\r\n\r\n\x06\x04\x07\x04\0\x02\0\x12\x03:\x10\x1a\n\x0e\n\x07\
    \x04\x07\x04\0\x02\0\x01\x12\x03:\x10\x15\n\x0e\n\x07\x04\x07\x04\0\x02\
    \0\x02\x12\x03:\x18\x19\n\r\n\x06\x04\x07\x04\0\x02\x01\x12\x03:\x1b'\n\
    \x0e\n\x07\x04\x07\x04\0\x02\x01\x01\x12\x03:\x1b\"\n\x0e\n\x07\x04\x07\
    \x04\0\x02\x01\x02\x12\x03:%&\n\r\n\x06\x04\x07\x04\0\x02\x02\x12\x03:(6\
    \n\x0e\n\x07\x04\x07\x04\0\x02\x02\x01\x12\x03:(1\n\x0e\n\x07\x04\x07\
    \x04\0\x02\x02\x02\x12\x03:45\n\r\n\x06\x04\x07\x04\0\x02\x03\x12\x03:7D\
    \n\x0e\n\x07\x04\x07\x04\0\x02\x03\x01\x12\x03:7?\n\x0e\n\x07\x04\x07\
    \x04\0\x02\x03\x02\x12\x03:BC\n\x0b\n\x04\x04\x07\x02\0\x12\x03<\x02\x1d\
    \n\x0c\n\x05\x04\x07\x02\0\x04\x12\x03<\x02\n\n\x0c\n\x05\x04\x07\x02\0\
    \x06\x12\x03<\x0b\x11\n\x0c\n\x05\x04\x07\x02\0\x01\x12\x03<\x12\x18\n\
    \x0c\n\x05\x04\x07\x02\0\x03\x12\x03<\x1b\x1c\n\x0b\n\x04\x04\x07\x02\
    \x01\x12\x03=\x02\x1d\n\x0c\n\x05\x04\x07\x02\x01\x04\x12\x03=\x02\n\n\
    \x0c\n\x05\x04\x07\x02\x01\x06\x12\x03=\x0b\x11\n\x0c\n\x05\x04\x07\x02\
    \x01\x01\x12\x03=\x12\x18\n\x0c\n\x05\x04\x07\x02\x01\x03\x12\x03=\x1b\
    \x1c\n\n\n\x02\x04\x08\x12\x04@\0I\x01\n\n\n\x03\x04\x08\x01\x12\x03@\
    \x08\x10\n\x0b\n\x04\x04\x08\x04\0\x12\x03A\x02:\n\x0c\n\x05\x04\x08\x04\
    \0\x01\x12\x03A\x07\r\n\r\n\x06\x04\x08\x04\0\x02\0\x12\x03A\x10\x1c\n\
    \x0e\n\x07\x04\x08\x04\0\x02\0\x01\x12\x03A\x10\x17\n\x0e\n\x07\x04\x08\
    \x04\0\x02\0\x02\x12\x03A\x1a\x1b\n\r\n\x06\x04\x08\x04\0\x02\x01\x12\
    \x03A\x1d*\n\x0e\n\x07\x04\x08\x04\0\x02\x01\x01\x12\x03A\x1d%\n\x0e\n\
    \x07\x04\x08\x04\0\x02\x01\x02\x12\x03A()\n\r\n\x06\x04\x08\x04\0\x02\
    \x02\x12\x03A+8\n\x0e\n\x07\x04\x08\x04\0\x02\x02\x01\x12\x03A+3\n\x0e\n\
    \x07\x04\x08\x04\0\x02\x02\x02\x12\x03A67\n\x0b\n\x04\x04\x08\x02\0\x12\
    \x03C\x02\x20\n\x0c\n\x05\x04\x08\x02\0\x04\x12\x03C\x02\n\n\x0c\n\x05\
    \x04\x08\x02\0\x05\x12\x03C\x0b\x11\n\x0c\n\x05\x04\x08\x02\0\x01\x12\
    \x03C\x12\x1b\n\x0c\n\x05\x04\x08\x02\0\x03\x12\x03C\x1e\x1f\n\x0b\n\x04\
    \x04\x08\x02\x01\x12\x03D\x02$\n\x0c\n\x05\x04\x08\x02\x01\x04\x12\x03D\
    \x02\n\n\x0c\n\x05\x04\x08\x02\x01\x05\x12\x03D\x0b\x11\n\x0c\n\x05\x04\
    \x08\x02\x01\x01\x12\x03D\x12\x1f\n\x0c\n\x05\x04\x08\x02\x01\x03\x12\
    \x03D\"#\n\x0b\n\x04\x04\x08\x02\x02\x12\x03E\x02\x1b\n\x0c\n\x05\x04\
    \x08\x02\x02\x04\x12\x03E\x02\n\n\x0c\n\x05\x04\x08\x02\x02\x05\x12\x03E\
    \x0b\x11\n\x0c\n\x05\x04\x08\x02\x02\x01\x12\x03E\x12\x16\n\x0c\n\x05\
    \x04\x08\x02\x02\x03\x12\x03E\x19\x1a\n\x0b\n\x04\x04\x08\x02\x03\x12\
    \x03F\x02\"\n\x0c\n\x05\x04\x08\x02\x03\x04\x12\x03F\x02\n\n\x0c\n\x05\
    \x04\x08\x02\x03\x05\x12\x03F\x0b\x11\n\x0c\n\x05\x04\x08\x02\x03\x01\
    \x12\x03F\x12\x1d\n\x0c\n\x05\x04\x08\x02\x03\x03\x12\x03F\x20!\n\x0b\n\
    \x04\x04\x08\x02\x04\x12\x03G\x02\x1d\n\x0c\n\x05\x04\x08\x02\x04\x04\
    \x12\x03G\x02\n\n\x0c\n\x05\x04\x08\x02\x04\x06\x12\x03G\x0b\x11\n\x0c\n\
    \x05\x04\x08\x02\x04\x01\x12\x03G\x12\x18\n\x0c\n\x05\x04\x08\x02\x04\
    \x03\x12\x03G\x1b\x1c\n\x0b\n\x04\x04\x08\x02\x05\x12\x03H\x02\x1c\n\x0c\
    \n\x05\x04\x08\x02\x05\x04\x12\x03H\x02\n\n\x0c\n\x05\x04\x08\x02\x05\
    \x05\x12\x03H\x0b\x11\n\x0c\n\x05\x04\x08\x02\x05\x01\x12\x03H\x12\x17\n\
    \x0c\n\x05\x04\x08\x02\x05\x03\x12\x03H\x1a\x1b\n\n\n\x02\x04\t\x12\x04K\
    \0S\x01\n\n\n\x03\x04\t\x01\x12\x03K\x08\x0f\n\x0b\n\x04\x04\t\x02\0\x12\
    \x03L\x02\x20\n\x0c\n\x05\x04\t\x02\0\x04\x12\x03L\x02\n\n\x0c\n\x05\x04\
    \t\x02\0\x05\x12\x03L\x0b\x11\n\x0c\n\x05\x04\t\x02\0\x01\x12\x03L\x12\
    \x1b\n\x0c\n\x05\x04\t\x02\0\x03\x12\x03L\x1e\x1f\n\x0b\n\x04\x04\t\x02\
    \x01\x12\x03M\x02$\n\x0c\n\x05\x04\t\x02\x01\x04\x12\x03M\x02\n\n\x0c\n\
    \x05\x04\t\x02\x01\x05\x12\x03M\x0b\x11\n\x0c\n\x05\x04\t\x02\x01\x01\
    \x12\x03M\x12\x1f\n\x0c\n\x05\x04\t\x02\x01\x03\x12\x03M\"#\n\x0b\n\x04\
    \x04\t\x02\x02\x12\x03N\x02\"\n\x0c\n\x05\x04\t\x02\x02\x04\x12\x03N\x02\
    \n\n\x0c\n\x05\x04\t\x02\x02\x05\x12\x03N\x0b\x11\n\x0c\n\x05\x04\t\x02\
    \x02\x01\x12\x03N\x12\x1d\n\x0c\n\x05\x04\t\x02\x02\x03\x12\x03N\x20!\n\
    \x0b\n\x04\x04\t\x02\x03\x12\x03O\x02\x20\n\x0c\n\x05\x04\t\x02\x03\x04\
    \x12\x03O\x02\n\n\x0c\n\x05\x04\t\x02\x03\x05\x12\x03O\x0b\x0f\n\x0c\n\
    \x05\x04\t\x02\x03\x01\x12\x03O\x10\x1b\n\x0c\n\x05\x04\t\x02\x03\x03\
    \x12\x03O\x1e\x1f\n\x0b\n\x04\x04\t\x02\x04\x12\x03P\x02\x1a\n\x0c\n\x05\
    \x04\t\x02\x04\x04\x12\x03P\x02\n\n\x0c\n\x05\x04\t\x02\x04\x05\x12\x03P\
    \x0b\x11\n\x0c\n\x05\x04\t\x02\x04\x01\x12\x03P\x12\x15\n\x0c\n\x05\x04\
    \t\x02\x04\x03\x12\x03P\x18\x19\n\x0b\n\x04\x04\t\x02\x05\x12\x03Q\x02\
    \x1a\n\x0c\n\x05\x04\t\x02\x05\x04\x12\x03Q\x02\n\n\x0c\n\x05\x04\t\x02\
    \x05\x05\x12\x03Q\x0b\x10\n\x0c\n\x05\x04\t\x02\x05\x01\x12\x03Q\x11\x14\
    \n\x0c\n\x05\x04\t\x02\x05\x03\x12\x03Q\x17\x19\n\x0b\n\x04\x04\t\x02\
    \x06\x12\x03R\x02\x1c\n\x0c\n\x05\x04\t\x02\x06\x04\x12\x03R\x02\n\n\x0c\
    \n\x05\x04\t\x02\x06\x06\x12\x03R\x0b\x12\n\x0c\n\x05\x04\t\x02\x06\x01\
    \x12\x03R\x13\x16\n\x0c\n\x05\x04\t\x02\x06\x03\x12\x03R\x19\x1b\n\n\n\
    \x02\x04\n\x12\x04U\0Z\x01\n\n\n\x03\x04\n\x01\x12\x03U\x08\x15\n\x0b\n\
    \x04\x04\n\x02\0\x12\x03V\x02$\n\x0c\n\x05\x04\n\x02\0\x04\x12\x03V\x02\
    \n\n\x0c\n\x05\x04\n\x02\0\x05\x12\x03V\x0b\x11\n\x0c\n\x05\x04\n\x02\0\
    \x01\x12\x03V\x12\x1f\n\x0c\n\x05\x04\n\x02\0\x03\x12\x03V\"#\n\x0b\n\
    \x04\x04\n\x02\x01\x12\x03W\x02\"\n\x0c\n\x05\x04\n\x02\x01\x04\x12\x03W\
    \x02\n\n\x0c\n\x05\x04\n\x02\x01\x05\x12\x03W\x0b\x11\n\x0c\n\x05\x04\n\
    \x02\x01\x01\x12\x03W\x12\x1d\n\x0c\n\x05\x04\n\x02\x01\x03\x12\x03W\x20\
    !\n\x0b\n\x04\x04\n\x02\x02\x12\x03X\x02\x1e\n\x0c\n\x05\x04\n\x02\x02\
    \x04\x12\x03X\x02\n\n\x0c\n\x05\x04\n\x02\x02\x05\x12\x03X\x0b\x0f\n\x0c\
    \n\x05\x04\n\x02\x02\x01\x12\x03X\x10\x19\n\x0c\n\x05\x04\n\x02\x02\x03\
    \x12\x03X\x1c\x1d\n\x0b\n\x04\x04\n\x02\x03\x12\x03Y\x02\x1c\n\x0c\n\x05\
    \x04\n\x02\x03\x04\x12\x03Y\x02\n\n\x0c\n\x05\x04\n\x02\x03\x05\x12\x03Y\
    \x0b\x10\n\x0c\n\x05\x04\n\x02\x03\x01\x12\x03Y\x11\x17\n\x0c\n\x05\x04\
    \n\x02\x03\x03\x12\x03Y\x1a\x1b\n\n\n\x02\x04\x0b\x12\x04\\\0b\x01\n\n\n\
    \x03\x04\x0b\x01\x12\x03\\\x08\x13\n\x0b\n\x04\x04\x0b\x02\0\x12\x03]\
    \x02$\n\x0c\n\x05\x04\x0b\x02\0\x04\x12\x03]\x02\n\n\x0c\n\x05\x04\x0b\
    \x02\0\x05\x12\x03]\x0b\x11\n\x0c\n\x05\x04\x0b\x02\0\x01\x12\x03]\x12\
    \x1f\n\x0c\n\x05\x04\x0b\x02\0\x03\x12\x03]\"#\n\x0b\n\x04\x04\x0b\x02\
    \x01\x12\x03^\x02\"\n\x0c\n\x05\x04\x0b\x02\x01\x04\x12\x03^\x02\n\n\x0c\
    \n\x05\x04\x0b\x02\x01\x05\x12\x03^\x0b\x11\n\x0c\n\x05\x04\x0b\x02\x01\
    \x01\x12\x03^\x12\x1d\n\x0c\n\x05\x04\x0b\x02\x01\x03\x12\x03^\x20!\n\
    \x0b\n\x04\x04\x0b\x02\x02\x12\x03_\x02\x1e\n\x0c\n\x05\x04\x0b\x02\x02\
    \x04\x12\x03_\x02\n\n\x0c\n\x05\x04\x0b\x02\x02\x05\x12\x03_\x0b\x0f\n\
    \x0c\n\x05\x04\x0b\x02\x02\x01\x12\x03_\x10\x19\n\x0c\n\x05\x04\x0b\x02\
    \x02\x03\x12\x03_\x1c\x1d\n\x0b\n\x04\x04\x0b\x02\x03\x12\x03`\x02\x1f\n\
    \x0c\n\x05\x04\x0b\x02\x03\x04\x12\x03`\x02\n\n\x0c\n\x05\x04\x0b\x02\
    \x03\x05\x12\x03`\x0b\x11\n\x0c\n\x05\x04\x0b\x02\x03\x01\x12\x03`\x12\
    \x1a\n\x0c\n\x05\x04\x0b\x02\x03\x03\x12\x03`\x1d\x1e\n\x0b\n\x04\x04\
    \x0b\x02\x04\x12\x03a\x02\x1a\n\x0c\n\x05\x04\x0b\x02\x04\x04\x12\x03a\
    \x02\n\n\x0c\n\x05\x04\x0b\x02\x04\x05\x12\x03a\x0b\x10\n\x0c\n\x05\x04\
    \x0b\x02\x04\x01\x12\x03a\x11\x15\n\x0c\n\x05\x04\x0b\x02\x04\x03\x12\
    \x03a\x18\x19\n\n\n\x02\x04\x0c\x12\x04d\0n\x01\n\n\n\x03\x04\x0c\x01\
    \x12\x03d\x08\x0f\n\x0b\n\x04\x04\x0c\x02\0\x12\x03e\x021\n\x0c\n\x05\
    \x04\x0c\x02\0\x04\x12\x03e\x02\n\n\x0c\n\x05\x04\x0c\x02\0\x05\x12\x03e\
    \x0b\x11\n\x0c\n\x05\x04\x0c\x02\0\x01\x12\x03e\x12\x14\n\x0c\n\x05\x04\
    \x0c\x02\0\x03\x12\x03e\x17\x18\n\x0c\n\x05\x04\x0c\x02\0\x08\x12\x03e\
    \x190\n\x0c\n\x05\x04\x0c\x02\0\x07\x12\x03e$/\n\x0b\n\x04\x04\x0c\x02\
    \x01\x12\x03f\x027\n\x0c\n\x05\x04\x0c\x02\x01\x04\x12\x03f\x02\n\n\x0c\
    \n\x05\x04\x0c\x02\x01\x05\x12\x03f\x0b\x11\n\x0c\n\x05\x04\x0c\x02\x01\
    \x01\x12\x03f\x12\x1a\n\x0c\n\x05\x04\x0c\x02\x01\x03\x12\x03f\x1d\x1e\n\
    \x0c\n\x05\x04\x0c\x02\x01\x08\x12\x03f\x1f6\n\x0c\n\x05\x04\x0c\x02\x01\
    \x07\x12\x03f*5\n\x0b\n\x04\x04\x0c\x02\x02\x12\x03g\x028\n\x0c\n\x05\
    \x04\x0c\x02\x02\x04\x12\x03g\x02\n\n\x0c\n\x05\x04\x0c\x02\x02\x05\x12\
    \x03g\x0b\x11\n\x0c\n\x05\x04\x0c\x02\x02\x01\x12\x03g\x12\x1b\n\x0c\n\
    \x05\x04\x0c\x02\x02\x03\x12\x03g\x1e\x1f\n\x0c\n\x05\x04\x0c\x02\x02\
    \x08\x12\x03g\x207\n\x0c\n\x05\x04\x0c\x02\x02\x07\x12\x03g+6\n\x0b\n\
    \x04\x04\x0c\x02\x03\x12\x03h\x02\"\n\x0c\n\x05\x04\x0c\x02\x03\x04\x12\
    \x03h\x02\n\n\x0c\n\x05\x04\x0c\x02\x03\x05\x12\x03h\x0b\x11\n\x0c\n\x05\
    \x04\x0c\x02\x03\x01\x12\x03h\x12\x1d\n\x0c\n\x05\x04\x0c\x02\x03\x03\
    \x12\x03h\x20!\n\x0b\n\x04\x04\x0c\x02\x04\x12\x03i\x02>\n\x0c\n\x05\x04\
    \x0c\x02\x04\x04\x12\x03i\x02\n\n\x0c\n\x05\x04\x0c\x02\x04\x05\x12\x03i\
    \x0b\x11\n\x0c\n\x05\x04\x0c\x02\x04\x01\x12\x03i\x12!\n\x0c\n\x05\x04\
    \x0c\x02\x04\x03\x12\x03i$%\n\x0c\n\x05\x04\x0c\x02\x04\x08\x12\x03i&=\n\
    \x0c\n\x05\x04\x0c\x02\x04\x07\x12\x03i1<\n\x0b\n\x04\x04\x0c\x02\x05\
    \x12\x03j\x02(\n\x0c\n\x05\x04\x0c\x02\x05\x04\x12\x03j\x02\n\n\x0c\n\
    \x05\x04\x0c\x02\x05\x05\x12\x03j\x0b\x11\n\x0c\n\x05\x04\x0c\x02\x05\
    \x01\x12\x03j\x12#\n\x0c\n\x05\x04\x0c\x02\x05\x03\x12\x03j&'\n\x0b\n\
    \x04\x04\x0c\x02\x06\x12\x03k\x02=\n\x0c\n\x05\x04\x0c\x02\x06\x04\x12\
    \x03k\x02\n\n\x0c\n\x05\x04\x0c\x02\x06\x05\x12\x03k\x0b\x11\n\x0c\n\x05\
    \x04\x0c\x02\x06\x01\x12\x03k\x12\x20\n\x0c\n\x05\x04\x0c\x02\x06\x03\
    \x12\x03k#$\n\x0c\n\x05\x04\x0c\x02\x06\x08\x12\x03k%<\n\x0c\n\x05\x04\
    \x0c\x02\x06\x07\x12\x03k0;\n\x0b\n\x04\x04\x0c\x02\x07\x12\x03l\x028\n\
    \x0c\n\x05\x04\x0c\x02\x07\x04\x12\x03l\x02\n\n\x0c\n\x05\x04\x0c\x02\
    \x07\x05\x12\x03l\x0b\x11\n\x0c\n\x05\x04\x0c\x02\x07\x01\x12\x03l\x12\"\
    \n\x0c\n\x05\x04\x0c\x02\x07\x03\x12\x03l%&\n\x0c\n\x05\x04\x0c\x02\x07\
    \x08\x12\x03l'7\n\x0c\n\x05\x04\x0c\x02\x07\x07\x12\x03l26\n\x0b\n\x04\
    \x04\x0c\x02\x08\x12\x03m\x020\n\x0c\n\x05\x04\x0c\x02\x08\x04\x12\x03m\
    \x02\n\n\x0c\n\x05\x04\x0c\x02\x08\x06\x12\x03m\x0b\x16\n\x0c\n\x05\x04\
    \x0c\x02\x08\x01\x12\x03m\x17+\n\x0c\n\x05\x04\x0c\x02\x08\x03\x12\x03m.\
    /\n\n\n\x02\x04\r\x12\x04p\0r\x01\n\n\n\x03\x04\r\x01\x12\x03p\x08\x11\n\
    \x0b\n\x04\x04\r\x02\0\x12\x03q\x02\x20\n\x0c\n\x05\x04\r\x02\0\x04\x12\
    \x03q\x02\n\n\x0c\n\x05\x04\r\x02\0\x05\x12\x03q\x0b\x11\n\x0c\n\x05\x04\
    \r\x02\0\x01\x12\x03q\x12\x1b\n\x0c\n\x05\x04\r\x02\0\x03\x12\x03q\x1e\
    \x1f\n\x0b\n\x02\x04\x0e\x12\x05t\0\x81\x01\x01\n\n\n\x03\x04\x0e\x01\
    \x12\x03t\x08\x0c\n\x0b\n\x04\x04\x0e\x04\0\x12\x03u\x02@\n\x0c\n\x05\
    \x04\x0e\x04\0\x01\x12\x03u\x07\x0b\n\r\n\x06\x04\x0e\x04\0\x02\0\x12\
    \x03u\x0e\x17\n\x0e\n\x07\x04\x0e\x04\0\x02\0\x01\x12\x03u\x0e\x12\n\x0e\
    \n\x07\x04\x0e\x04\0\x02\0\x02\x12\x03u\x15\x16\n\r\n\x06\x04\x0e\x04\0\
    \x02\x01\x12\x03u\x18\x20\n\x0e\n\x07\x04\x0e\x04\0\x02\x01\x01\x12\x03u\
    \x18\x1b\n\x0e\n\x07\x04\x0e\x04\0\x02\x01\x02\x12\x03u\x1e\x1f\n\r\n\
    \x06\x04\x0e\x04\0\x02\x02\x12\x03u!-\n\x0e\n\x07\x04\x0e\x04\0\x02\x02\
    \x01\x12\x03u!(\n\x0e\n\x07\x04\x0e\x04\0\x02\x02\x02\x12\x03u+,\n\r\n\
    \x06\x04\x0e\x04\0\x02\x03\x12\x03u.>\n\x0e\n\x07\x04\x0e\x04\0\x02\x03\
    \x01\x12\x03u.9\n\x0e\n\x07\x04\x0e\x04\0\x02\x03\x02\x12\x03u<=\n3\n\
    \x04\x04\x0e\x02\0\x12\x03x\x02\x19\x1a&\x20Identifies\x20which\x20field\
    \x20is\x20filled\x20in.\n\n\x0c\n\x05\x04\x0e\x02\0\x04\x12\x03x\x02\n\n\
    \x0c\n\x05\x04\x0e\x02\0\x06\x12\x03x\x0b\x0f\n\x0c\n\x05\x04\x0e\x02\0\
    \x01\x12\x03x\x10\x14\n\x0c\n\x05\x04\x0e\x02\0\x03\x12\x03x\x17\x18\n\
    \x0c\n\x04\x04\x0e\x08\0\x12\x04y\x02~\x03\n\x0c\n\x05\x04\x0e\x08\0\x01\
    \x12\x03y\x08\x0f\n\x0b\n\x04\x04\x0e\x02\x01\x12\x03z\x04\x12\n\x0c\n\
    \x05\x04\x0e\x02\x01\x06\x12\x03z\x04\x08\n\x0c\n\x05\x04\x0e\x02\x01\
    \x01\x12\x03z\t\r\n\x0c\n\x05\x04\x0e\x02\x01\x03\x12\x03z\x10\x11\n\x0b\
    \n\x04\x04\x0e\x02\x02\x12\x03{\x04\x10\n\x0c\n\x05\x04\x0e\x02\x02\x06\
    \x12\x03{\x04\x07\n\x0c\n\x05\x04\x0e\x02\x02\x01\x12\x03{\x08\x0b\n\x0c\
    \n\x05\x04\x0e\x02\x02\x03\x12\x03{\x0e\x0f\n\x0b\n\x04\x04\x0e\x02\x03\
    \x12\x03|\x04\x18\n\x0c\n\x05\x04\x0e\x02\x03\x06\x12\x03|\x04\x0b\n\x0c\
    \n\x05\x04\x0e\x02\x03\x01\x12\x03|\x0c\x13\n\x0c\n\x05\x04\x0e\x02\x03\
    \x03\x12\x03|\x16\x17\n\x0b\n\x04\x04\x0e\x02\x04\x12\x03}\x04\x1f\n\x0c\
    \n\x05\x04\x0e\x02\x04\x06\x12\x03}\x04\x0e\n\x0c\n\x05\x04\x0e\x02\x04\
    \x01\x12\x03}\x0f\x1a\n\x0c\n\x05\x04\x0e\x02\x04\x03\x12\x03}\x1d\x1e\n\
    \x0b\n\x04\x04\x0e\x02\x05\x12\x03\x7f\x02%\n\x0c\n\x05\x04\x0e\x02\x05\
    \x04\x12\x03\x7f\x02\n\n\x0c\n\x05\x04\x0e\x02\x05\x06\x12\x03\x7f\x0b\
    \x15\n\x0c\n\x05\x04\x0e\x02\x05\x01\x12\x03\x7f\x16\x20\n\x0c\n\x05\x04\
    \x0e\x02\x05\x03\x12\x03\x7f#$\n\x0c\n\x04\x04\x0e\x02\x06\x12\x04\x80\
    \x01\x02\x1a\n\r\n\x05\x04\x0e\x02\x06\x04\x12\x04\x80\x01\x02\n\n\r\n\
    \x05\x04\x0e\x02\x06\x06\x12\x04\x80\x01\x0b\x0f\n\r\n\x05\x04\x0e\x02\
    \x06\x01\x12\x04\x80\x01\x10\x15\n\r\n\x05\x04\x0e\x02\x06\x03\x12\x04\
    \x80\x01\x18\x19\n\x0c\n\x02\x04\x0f\x12\x06\x83\x01\0\x9d\x01\x01\n\x0b\
    \n\x03\x04\x0f\x01\x12\x04\x83\x01\x08\r\n\x0e\n\x04\x04\x0f\x04\0\x12\
    \x06\x84\x01\x02\x8f\x01\x03\n\r\n\x05\x04\x0f\x04\0\x01\x12\x04\x84\x01\
    \x07\x0b\n\x0e\n\x06\x04\x0f\x04\0\x02\0\x12\x04\x85\x01\x04\x0f\n\x0f\n\
    \x07\x04\x0f\x04\0\x02\0\x01\x12\x04\x85\x01\x04\n\n\x0f\n\x07\x04\x0f\
    \x04\0\x02\0\x02\x12\x04\x85\x01\r\x0e\n\x0e\n\x06\x04\x0f\x04\0\x02\x01\
    \x12\x04\x86\x01\x04\x10\n\x0f\n\x07\x04\x0f\x04\0\x02\x01\x01\x12\x04\
    \x86\x01\x04\x0b\n\x0f\n\x07\x04\x0f\x04\0\x02\x01\x02\x12\x04\x86\x01\
    \x0e\x0f\n\x0e\n\x06\x04\x0f\x04\0\x02\x02\x12\x04\x87\x01\x04\x11\n\x0f\
    \n\x07\x04\x0f\x04\0\x02\x02\x01\x12\x04\x87\x01\x04\x0c\n\x0f\n\x07\x04\
    \x0f\x04\0\x02\x02\x02\x12\x04\x87\x01\x0f\x10\n\x0e\n\x06\x04\x0f\x04\0\
    \x02\x03\x12\x04\x88\x01\x04\x16\n\x0f\n\x07\x04\x0f\x04\0\x02\x03\x01\
    \x12\x04\x88\x01\x04\x11\n\x0f\n\x07\x04\x0f\x04\0\x02\x03\x02\x12\x04\
    \x88\x01\x14\x15\n\x0e\n\x06\x04\x0f\x04\0\x02\x04\x12\x04\x89\x01\x04\
    \x14\n\x0f\n\x07\x04\x0f\x04\0\x02\x04\x01\x12\x04\x89\x01\x04\x0f\n\x0f\
    \n\x07\x04\x0f\x04\0\x02\x04\x02\x12\x04\x89\x01\x12\x13\n\x0e\n\x06\x04\
    \x0f\x04\0\x02\x05\x12\x04\x8a\x01\x04\r\n\x0f\n\x07\x04\x0f\x04\0\x02\
    \x05\x01\x12\x04\x8a\x01\x04\x08\n\x0f\n\x07\x04\x0f\x04\0\x02\x05\x02\
    \x12\x04\x8a\x01\x0b\x0c\n\x0e\n\x06\x04\x0f\x04\0\x02\x06\x12\x04\x8b\
    \x01\x04\x0e\n\x0f\n\x07\x04\x0f\x04\0\x02\x06\x01\x12\x04\x8b\x01\x04\t\
    \n\x0f\n\x07\x04\x0f\x04\0\x02\x06\x02\x12\x04\x8b\x01\x0c\r\n\x0e\n\x06\
    \x04\x0f\x04\0\x02\x07\x12\x04\x8c\x01\x04\x17\n\x0f\n\x07\x04\x0f\x04\0\
    \x02\x07\x01\x12\x04\x8c\x01\x04\x12\n\x0f\n\x07\x04\x0f\x04\0\x02\x07\
    \x02\x12\x04\x8c\x01\x15\x16\n\x0e\n\x06\x04\x0f\x04\0\x02\x08\x12\x04\
    \x8d\x01\x04\x12\n\x0f\n\x07\x04\x0f\x04\0\x02\x08\x01\x12\x04\x8d\x01\
    \x04\r\n\x0f\n\x07\x04\x0f\x04\0\x02\x08\x02\x12\x04\x8d\x01\x10\x11\n\
    \x0e\n\x06\x04\x0f\x04\0\x02\t\x12\x04\x8e\x01\x04\x0e\n\x0f\n\x07\x04\
    \x0f\x04\0\x02\t\x01\x12\x04\x8e\x01\x04\x08\n\x0f\n\x07\x04\x0f\x04\0\
    \x02\t\x02\x12\x04\x8e\x01\x0b\r\n\x0c\n\x04\x04\x0f\x02\0\x12\x04\x91\
    \x01\x02\x19\n\r\n\x05\x04\x0f\x02\0\x04\x12\x04\x91\x01\x02\n\n\r\n\x05\
    \x04\x0f\x02\0\x06\x12\x04\x91\x01\x0b\x0f\n\r\n\x05\x04\x0f\x02\0\x01\
    \x12\x04\x91\x01\x10\x14\n\r\n\x05\x04\x0f\x02\0\x03\x12\x04\x91\x01\x17\
    \x18\n\x0c\n\x04\x04\x0f\x02\x01\x12\x04\x92\x01\x02\x1a\n\r\n\x05\x04\
    \x0f\x02\x01\x04\x12\x04\x92\x01\x02\n\n\r\n\x05\x04\x0f\x02\x01\x05\x12\
    \x04\x92\x01\x0b\x11\n\r\n\x05\x04\x0f\x02\x01\x01\x12\x04\x92\x01\x12\
    \x15\n\r\n\x05\x04\x0f\x02\x01\x03\x12\x04\x92\x01\x18\x19\n\x0c\n\x04\
    \x04\x0f\x02\x02\x12\x04\x93\x01\x02\x1e\n\r\n\x05\x04\x0f\x02\x02\x04\
    \x12\x04\x93\x01\x02\n\n\r\n\x05\x04\x0f\x02\x02\x05\x12\x04\x93\x01\x0b\
    \x11\n\r\n\x05\x04\x0f\x02\x02\x01\x12\x04\x93\x01\x12\x19\n\r\n\x05\x04\
    \x0f\x02\x02\x03\x12\x04\x93\x01\x1c\x1d\n\x0e\n\x04\x04\x0f\x08\0\x12\
    \x06\x94\x01\x02\x9c\x01\x03\n\r\n\x05\x04\x0f\x08\0\x01\x12\x04\x94\x01\
    \x08\x0f\n\x0c\n\x04\x04\x0f\x02\x03\x12\x04\x95\x01\x04\x1a\n\r\n\x05\
    \x04\x0f\x02\x03\x06\x12\x04\x95\x01\x04\x0e\n\r\n\x05\x04\x0f\x02\x03\
    \x01\x12\x04\x95\x01\x0f\x15\n\r\n\x05\x04\x0f\x02\x03\x03\x12\x04\x95\
    \x01\x18\x19\n\x0c\n\x04\x04\x0f\x02\x04\x12\x04\x96\x01\x04\x18\n\r\n\
    \x05\x04\x0f\x02\x04\x06\x12\x04\x96\x01\x04\x0b\n\r\n\x05\x04\x0f\x02\
    \x04\x01\x12\x04\x96\x01\x0c\x13\n\r\n\x05\x04\x0f\x02\x04\x03\x12\x04\
    \x96\x01\x16\x17\n\x0c\n\x04\x04\x0f\x02\x05\x12\x04\x97\x01\x04%\n\r\n\
    \x05\x04\x0f\x02\x05\x06\x12\x04\x97\x01\x04\x11\n\r\n\x05\x04\x0f\x02\
    \x05\x01\x12\x04\x97\x01\x12\x20\n\r\n\x05\x04\x0f\x02\x05\x03\x12\x04\
    \x97\x01#$\n\x0c\n\x04\x04\x0f\x02\x06\x12\x04\x98\x01\x04!\n\r\n\x05\
    \x04\x0f\x02\x06\x06\x12\x04\x98\x01\x04\x0f\n\r\n\x05\x04\x0f\x02\x06\
    \x01\x12\x04\x98\x01\x10\x1c\n\r\n\x05\x04\x0f\x02\x06\x03\x12\x04\x98\
    \x01\x1f\x20\n\x0c\n\x04\x04\x0f\x02\x07\x12\x04\x99\x01\x04\x1a\n\r\n\
    \x05\x04\x0f\x02\x07\x06\x12\x04\x99\x01\x04\x0c\n\r\n\x05\x04\x0f\x02\
    \x07\x01\x12\x04\x99\x01\r\x15\n\r\n\x05\x04\x0f\x02\x07\x03\x12\x04\x99\
    \x01\x18\x19\n\x0c\n\x04\x04\x0f\x02\x08\x12\x04\x9a\x01\x04\x1c\n\r\n\
    \x05\x04\x0f\x02\x08\x06\x12\x04\x9a\x01\x04\r\n\r\n\x05\x04\x0f\x02\x08\
    \x01\x12\x04\x9a\x01\x0e\x17\n\r\n\x05\x04\x0f\x02\x08\x03\x12\x04\x9a\
    \x01\x1a\x1b\n\x0c\n\x04\x04\x0f\x02\t\x12\x04\x9b\x01\x04\x13\n\r\n\x05\
    \x04\x0f\x02\t\x06\x12\x04\x9b\x01\x04\x08\n\r\n\x05\x04\x0f\x02\t\x01\
    \x12\x04\x9b\x01\t\r\n\r\n\x05\x04\x0f\x02\t\x03\x12\x04\x9b\x01\x10\x12\
    \n\x0c\n\x02\x04\x10\x12\x06\x9f\x01\0\xa3\x01\x01\n\x0b\n\x03\x04\x10\
    \x01\x12\x04\x9f\x01\x08\x0c\n\x0c\n\x04\x04\x10\x02\0\x12\x04\xa0\x01\
    \x02\x1e\n\r\n\x05\x04\x10\x02\0\x04\x12\x04\xa0\x01\x02\n\n\r\n\x05\x04\
    \x10\x02\0\x05\x12\x04\xa0\x01\x0b\x0f\n\r\n\x05\x04\x10\x02\0\x01\x12\
    \x04\xa0\x01\x10\x19\n\r\n\x05\x04\x10\x02\0\x03\x12\x04\xa0\x01\x1c\x1d\
    \n\x0c\n\x04\x04\x10\x02\x01\x12\x04\xa1\x01\x02\x1b\n\r\n\x05\x04\x10\
    \x02\x01\x04\x12\x04\xa1\x01\x02\n\n\r\n\x05\x04\x10\x02\x01\x05\x12\x04\
    \xa1\x01\x0b\x10\n\r\n\x05\x04\x10\x02\x01\x01\x12\x04\xa1\x01\x11\x16\n\
    \r\n\x05\x04\x10\x02\x01\x03\x12\x04\xa1\x01\x19\x1a\n\x0c\n\x04\x04\x10\
    \x02\x02\x12\x04\xa2\x01\x02\x1d\n\r\n\x05\x04\x10\x02\x02\x04\x12\x04\
    \xa2\x01\x02\n\n\r\n\x05\x04\x10\x02\x02\x05\x12\x04\xa2\x01\x0b\x10\n\r\
    \n\x05\x04\x10\x02\x02\x01\x12\x04\xa2\x01\x11\x18\n\r\n\x05\x04\x10\x02\
    \x02\x03\x12\x04\xa2\x01\x1b\x1c\
";

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}
