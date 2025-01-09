use actix_web::{web, HttpResponse, Scope};

use crate::handlers;

pub fn user_routes() -> Scope {
    web::scope("/user")
        .service(handlers::user_handler)
        .route("/health", web::get().to(handlers::health_check))
}

pub fn storage_routes() -> Scope {
    web::scope("/storage")
        .service(handlers::storage_handler)
        .route("/health", web::get().to(handlers::health_check))
}

pub fn mempool_routes() -> Scope {
    web::scope("/mempool")
        .service(handlers::mempool_handler)
        .route("/health", web::get().to(handlers::health_check))
}

pub fn miner_routes() -> Scope {
    web::scope("/miner")
        .service(handlers::miner_handler)
        .route("/health", web::get().to(handlers::health_check))
}

pub fn miner_user_routes() -> Scope {
    web::scope("/miner_user")
        .service(handlers::miner_user_handler)
        .route("/health", web::get().to(handlers::health_check))
}