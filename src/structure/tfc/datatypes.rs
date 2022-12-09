use super::{
    datetime::{datetime_to_storage, storage_to_datetime},
    decimal::{decimal_to_storage, storage_to_decimal},
    integer::{bigint_to_storage, storage_to_bigint},
    TypedDictEntry,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::NaiveDateTime;
use num_derive::FromPrimitive;
use rug::Integer;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, FromPrimitive, Hash)]
pub enum Datatype {
    String = 0,
    UInt32,
    Int32,
    Float32,
    UInt64,
    Int64,
    Float64,
    Decimal,
    BigInt,
    Boolean,
    LangString,
    DateTime,
    Date,
    AnyURI,
    Language,
    NormalizedString,
    Token,
    NMToken,
    Name,
    NCName,
}

impl Datatype {
    pub fn cast<T: TdbDataType, B: Buf>(self, b: B) -> T {
        if T::datatype() != self {
            panic!("not the right datatype");
        }

        T::from_lexical(b)
    }

    pub fn record_size(&self) -> Option<u8> {
        match self {
            Datatype::Boolean => Some(4), // this is huge
            Datatype::String => None,
            Datatype::UInt32 => Some(4),
            Datatype::Int32 => Some(4),
            Datatype::UInt64 => Some(8),
            Datatype::Int64 => Some(8),
            Datatype::Float32 => Some(4),
            Datatype::Float64 => Some(8),
            Datatype::Decimal => None,
            Datatype::BigInt => None,
            Datatype::Token => None,
            Datatype::LangString => None,
            _ => None,
        }
    }
}

pub trait TdbDataType: FromLexical<Self> + ToLexical<Self> {
    fn datatype() -> Datatype;

    fn make_entry<T>(val: &T) -> TypedDictEntry
    where
        T: ToLexical<Self> + ?Sized,
    {
        TypedDictEntry::new(Self::datatype(), val.to_lexical().into())
    }
}

pub trait ToLexical<T: ?Sized> {
    fn to_lexical(&self) -> Bytes;
}

pub trait FromLexical<T: ?Sized> {
    fn from_lexical<B: Buf>(b: B) -> Self;
}

impl<T: AsRef<str>> ToLexical<String> for T {
    fn to_lexical(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_ref().as_bytes())
    }
}

impl FromLexical<String> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let mut vec = vec![0; b.remaining()];
        b.copy_to_slice(&mut vec);
        String::from_utf8(vec).unwrap()
    }
}

impl TdbDataType for String {
    fn datatype() -> Datatype {
        Datatype::String
    }
}

impl TdbDataType for u32 {
    fn datatype() -> Datatype {
        Datatype::UInt32
    }
}

impl FromLexical<u32> for u32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u32::<BigEndian>().unwrap()
    }
}

impl ToLexical<u32> for u32 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }
}

const I32_BYTE_MASK: u32 = 0b1000_0000 << (3 * 8);
impl TdbDataType for i32 {
    fn datatype() -> Datatype {
        Datatype::Int32
    }
}

impl FromLexical<i32> for i32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u32::<BigEndian>().unwrap();
        (I32_BYTE_MASK ^ i) as i32
    }
}

impl ToLexical<i32> for i32 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I32_BYTE_MASK ^ (*self as u32);
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

impl TdbDataType for u64 {
    fn datatype() -> Datatype {
        Datatype::UInt64
    }
}

impl FromLexical<u64> for u64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        b.reader().read_u64::<BigEndian>().unwrap()
    }
}

impl ToLexical<u64> for u64 {
    fn to_lexical(&self) -> Bytes {
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(*self).unwrap();

        buf.into_inner().freeze()
    }
}

const I64_BYTE_MASK: u64 = 0b1000_0000 << (7 * 8);
impl TdbDataType for i64 {
    fn datatype() -> Datatype {
        Datatype::Int64
    }
}

impl FromLexical<i64> for i64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u64::<BigEndian>().unwrap();
        (I64_BYTE_MASK ^ i) as i64
    }
}

impl ToLexical<i64> for i64 {
    fn to_lexical(&self) -> Bytes {
        let sign_flip = I64_BYTE_MASK ^ (*self as u64);
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(sign_flip).unwrap();
        buf.into_inner().freeze()
    }
}

const F32_SIGN_MASK: u32 = 0x8000_0000;
const F32_COMPLEMENT: u32 = 0xffff_ffff;
impl TdbDataType for f32 {
    fn datatype() -> Datatype {
        Datatype::Float32
    }
}

impl FromLexical<f32> for f32 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u32::<BigEndian>().unwrap();
        if i & F32_SIGN_MASK > 0 {
            f32::from_bits(i ^ F32_SIGN_MASK)
        } else {
            f32::from_bits(i ^ F32_COMPLEMENT)
        }
    }
}

impl ToLexical<f32> for f32 {
    fn to_lexical(&self) -> Bytes {
        let f = *self;
        let g: u32 = if f.signum() == -1.0 {
            f.to_bits() ^ F32_COMPLEMENT
        } else {
            f.to_bits() ^ F32_SIGN_MASK
        };
        let mut buf = BytesMut::new().writer();
        buf.write_u32::<BigEndian>(g).unwrap();
        buf.into_inner().freeze()
    }
}

const F64_SIGN_MASK: u64 = 0x8000_0000_0000_0000;
const F64_COMPLEMENT: u64 = 0xffff_ffff_ffff_ffff;
impl TdbDataType for f64 {
    fn datatype() -> Datatype {
        Datatype::Float64
    }
}

impl FromLexical<f64> for f64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let i = b.reader().read_u64::<BigEndian>().unwrap();
        if i & F64_SIGN_MASK > 0 {
            f64::from_bits(i ^ F64_SIGN_MASK)
        } else {
            f64::from_bits(i ^ F64_COMPLEMENT)
        }
    }
}

impl ToLexical<f64> for f64 {
    fn to_lexical(&self) -> Bytes {
        let f = *self;
        let g: u64 = if f.signum() == -1.0 {
            f.to_bits() ^ F64_COMPLEMENT
        } else {
            f.to_bits() ^ F64_SIGN_MASK
        };
        let mut buf = BytesMut::new().writer();
        buf.write_u64::<BigEndian>(g).unwrap();
        buf.into_inner().freeze()
    }
}

impl TdbDataType for Integer {
    fn datatype() -> Datatype {
        Datatype::BigInt
    }
}

impl FromLexical<Integer> for Integer {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        storage_to_bigint(&mut b)
    }
}

impl FromLexical<Integer> for String {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        // TODO make this better
        storage_to_bigint(&mut b).to_string()
    }
}

impl ToLexical<Integer> for Integer {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(bigint_to_storage(self.clone()))
    }
}

#[derive(PartialEq, Debug)]
pub struct Decimal(pub String);

impl TdbDataType for Decimal {
    fn datatype() -> Datatype {
        Datatype::Decimal
    }
}

impl FromLexical<Decimal> for Decimal {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        Decimal(storage_to_decimal(&mut b))
    }
}

impl FromLexical<Decimal> for String {
    fn from_lexical<B: Buf>(b: B) -> Self {
        // TODO make this better
        Decimal::from_lexical(b).0
    }
}

/*
impl FromLexical<Decimal> for f64 {
    fn from_lexical<B: Buf>(b: B) -> Self {
        let s = Decimal::from_lexical(b).0;
        s.parse::<f64>()
            .expect("Too much precision for cast from decimal to f64")
    }
}
*/

impl ToLexical<Decimal> for Decimal {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(decimal_to_storage(&self.0))
    }
}

impl TdbDataType for bool {
    fn datatype() -> Datatype {
        Datatype::Boolean
    }
}

impl FromLexical<bool> for bool {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        let num = b.get_u8();
        if num == 0 {
            false
        } else {
            true
        }
    }
}

impl ToLexical<bool> for bool {
    fn to_lexical(&self) -> Bytes {
        if *self {
            vec![1].into()
        } else {
            vec![0].into()
        }
    }
}

macro_rules! stringy_type {
    ($ty:ident) => {
        stringy_type!($ty, $ty);
    };
    ($ty:ident, $datatype:ident) => {
        #[derive(PartialEq, Debug)]
        pub struct $ty(String);

        impl AsRef<str> for $ty {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl TdbDataType for $ty {
            fn datatype() -> Datatype {
                Datatype::$datatype
            }
        }

        impl<T: AsRef<str>> ToLexical<$ty> for T {
            fn to_lexical(&self) -> Bytes {
                Bytes::copy_from_slice(self.as_ref().as_bytes())
            }
        }

        impl FromLexical<$ty> for $ty {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                let mut vec = vec![0; b.remaining()];
                b.copy_to_slice(&mut vec);
                $ty(String::from_utf8(vec).unwrap())
            }
        }

        impl FromLexical<$ty> for String {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                let mut vec = vec![0; b.remaining()];
                b.copy_to_slice(&mut vec);
                String::from_utf8(vec).unwrap()
            }
        }
    };
}

/*
macro_rules! biginty_type {
    ($ty:ident) => {
        biginty_type!($ty, $ty);
    };
    ($ty:ident, $datatype:ident) => {
        #[derive(PartialEq, Debug)]
        pub struct $ty(Integer);

        impl TdbDataType for $ty {
            fn datatype() -> Datatype {
                Datatype::$datatype
            }
        }

        impl FromLexical<$ty> for $ty {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                $ty(storage_to_bigint(&mut b).to_string())
            }
        }

        impl FromLexical<$ty> for String {
            fn from_lexical<B: Buf>(mut b: B) -> Self {
                $ty(storage_to_bigint(&mut b).to_string())
            }
        }

        impl ToLexical<$ty> for $ty {
            fn to_lexical(&self) -> Bytes {
                Bytes::from(bigint_to_storage(self.0.clone()))
            }
        }
    };
}
*/

stringy_type!(LangString);
stringy_type!(NCName);
stringy_type!(Name);
stringy_type!(Token);
stringy_type!(NMToken);
stringy_type!(NormalizedString);
stringy_type!(Language);
stringy_type!(AnyURI);

/*
biginty_type!(PositiveInteger);
biginty_type!(NonNegativeInteger);
biginty_type!(NegativeInteger);
biginty_type!(NonPositiveInteger);
*/

impl TdbDataType for NaiveDateTime {
    fn datatype() -> Datatype {
        Datatype::DateTime
    }
}

impl ToLexical<NaiveDateTime> for NaiveDateTime {
    fn to_lexical(&self) -> Bytes {
        Bytes::from(datetime_to_storage(self))
    }
}

impl FromLexical<NaiveDateTime> for NaiveDateTime {
    fn from_lexical<B: Buf>(mut b: B) -> Self {
        storage_to_datetime(&mut b)
    }
}
