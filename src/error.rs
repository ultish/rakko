use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("config error: {0}")]
    Config(String),

    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("toml decode error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("toml encode error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("schema registry error: {0}")]
    SchemaRegistry(String),

    #[error("{0}")]
    Other(String),
}

pub type AppResult<T> = Result<T, AppError>;
