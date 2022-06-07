use qwal::WalFile;
#[async_std::main]
async fn main() {
    let file = std::env::args()
        .skip(1)
        .next()
        .expect("One argument needs to be supplie");
    print!("File: {file}");

    WalFile::inspect::<_, Vec<u8>>(&file)
        .await
        .expect("could not open file");
}
