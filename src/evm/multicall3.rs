use anyhow::anyhow;
use ethabi::{ Function, Param, ParamType, StateMutability, Token };
use ethereum_types::{ H160, U256 };
use std::collections::HashMap;

use super::rpc::RpcClient;

pub const MULTICALL3_ADDR: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";

fn fn_aggregate3() -> Function {
    // aggregate3((address,bool,bytes)[]) returns ( (bool,bytes)[] )
    Function {
        name: "aggregate3".to_string(),
        inputs: vec![Param {
            name: "calls".to_string(),
            kind: ParamType::Array(
                Box::new(
                    ParamType::Tuple(
                        vec![
                            ParamType::Address, // target
                            ParamType::Bool, // allowFailure
                            ParamType::Bytes // callData
                        ]
                    )
                )
            ),
            internal_type: None,
        }],
        outputs: vec![Param {
            name: "returnData".to_string(),
            kind: ParamType::Array(
                Box::new(
                    ParamType::Tuple(
                        vec![
                            ParamType::Bool, // success
                            ParamType::Bytes // returnData
                        ]
                    )
                )
            ),
            internal_type: None,
        }],
        constant: None,
        state_mutability: StateMutability::Payable,
    }
}

fn fn_decimals() -> Function {
    Function {
        name: "decimals".to_string(),
        inputs: vec![],
        outputs: vec![Param {
            name: "decimals".to_string(),
            kind: ParamType::Uint(8),
            internal_type: None,
        }],
        constant: None,
        state_mutability: StateMutability::View,
    }
}

fn u8_from_return_data(bytes: &[u8]) -> Option<u8> {
    if bytes.is_empty() {
        return None;
    }
    // ABI uint8 is still 32-byte word, take last byte safely
    Some(*bytes.last()?)
}

fn fn_get_eth_balance() -> Function {
    Function {
        name: "getEthBalance".to_string(),
        inputs: vec![Param {
            name: "addr".to_string(),
            kind: ParamType::Address,
            internal_type: None,
        }],
        outputs: vec![Param {
            name: "balance".to_string(),
            kind: ParamType::Uint(256),
            internal_type: None,
        }],
        constant: None,
        state_mutability: StateMutability::View,
    }
}

fn fn_balance_of() -> Function {
    Function {
        name: "balanceOf".to_string(),
        inputs: vec![Param {
            name: "account".to_string(),
            kind: ParamType::Address,
            internal_type: None,
        }],
        outputs: vec![Param {
            name: "balance".to_string(),
            kind: ParamType::Uint(256),
            internal_type: None,
        }],
        constant: Some(true),
        state_mutability: StateMutability::View,
    }
}

fn parse_h160(addr: &str) -> Result<H160, anyhow::Error> {
    let a = addr.trim();
    let a = a.strip_prefix("0x").unwrap_or(a);
    let bytes = hex::decode(a)?;
    if bytes.len() != 20 {
        return Err(anyhow!("invalid address len: {} for {}", bytes.len(), addr));
    }
    Ok(H160::from_slice(&bytes))
}

fn u256_from_return_data(bytes: &[u8]) -> U256 {
    // expects 32 bytes, but tolerate weirdness
    if bytes.len() >= 32 {
        U256::from_big_endian(&bytes[bytes.len() - 32..])
    } else {
        let mut padded = vec![0u8; 32 - bytes.len()];
        padded.extend_from_slice(bytes);
        U256::from_big_endian(&padded)
    }
}

pub struct EvmBalances {
    pub native: HashMap<String, U256>, // wallet -> native
    pub erc20: HashMap<String, HashMap<String, U256>>, // wallet -> token -> bal
}

pub async fn fetch_token_decimals_multicall3(
    rpc: &RpcClient,
    token_contracts: &[String],
    max_calls_per_batch: usize
) -> Result<HashMap<String, u32>, anyhow::Error> {
    let agg = fn_aggregate3();
    let decimals_fn = fn_decimals();

    let mut call_tokens: Vec<Token> = vec![];
    let mut metas: Vec<String> = vec![];

    for t in token_contracts {
        // call decimals() on token contract
        let t_addr = parse_h160(t)?;
        let calldata = decimals_fn.encode_input(&[])?;

        metas.push(t.clone());
        call_tokens.push(
            Token::Tuple(vec![Token::Address(t_addr), Token::Bool(true), Token::Bytes(calldata)])
        );
    }

    let mut out: HashMap<String, u32> = HashMap::new();

    let mut i = 0usize;
    while i < call_tokens.len() {
        let end = (i + max_calls_per_batch).min(call_tokens.len());
        let chunk = call_tokens[i..end].to_vec();
        let chunk_meta = metas[i..end].to_vec();

        let input = agg.encode_input(&[Token::Array(chunk)])?;
        let data_hex = format!("0x{}", hex::encode(input));

        let raw = rpc.eth_call(MULTICALL3_ADDR, &data_hex).await?;
        let raw = raw.strip_prefix("0x").unwrap_or(&raw);
        let bytes = hex::decode(raw)?;

        let decoded = agg.decode_output(&bytes)?;
        let results = match decoded.get(0) {
            Some(Token::Array(arr)) => arr,
            _ => {
                return Err(anyhow!("unexpected aggregate3 decode shape for decimals"));
            }
        };

        for (idx, item) in results.iter().enumerate() {
            let token_addr = &chunk_meta[idx];

            let (success, returndata) = match item {
                Token::Tuple(items) if items.len() == 2 => {
                    let s = matches!(items[0], Token::Bool(true));
                    let b = match &items[1] {
                        Token::Bytes(bb) => bb.clone(),
                        _ => vec![],
                    };
                    (s, b)
                }
                _ => (false, vec![]),
            };

            let dec = if success {
                u8_from_return_data(&returndata).map(|x| x as u32)
            } else {
                None
            };

            // Default fallback if decimals() fails:
            out.insert(token_addr.clone(), dec.unwrap_or(18));
        }

        i = end;
    }

    Ok(out)
}

pub async fn fetch_balances_multicall3(
    rpc: &RpcClient,
    wallets: &[String],
    token_contracts: &[String],
    max_calls_per_batch: usize
) -> Result<EvmBalances, anyhow::Error> {
    let agg = fn_aggregate3();
    let get_eth = fn_get_eth_balance();
    let bal_of = fn_balance_of();

    // Build calls in a stable order so decoding aligns
    // Call layout:
    // 1) native for each wallet via getEthBalance
    // 2) balanceOf for each token for each wallet (wallet-major)
    #[derive(Clone)]
    struct CallMeta {
        kind: &'static str, // "native" | "erc20"
        wallet: String,
        token: Option<String>,
    }

    let mut call_metas: Vec<CallMeta> = vec![];
    let mut call_tokens: Vec<Token> = vec![];

    // native calls
    for w in wallets {
        let w_addr = parse_h160(w)?;
        let calldata = get_eth.encode_input(&[Token::Address(w_addr)])?;
        call_metas.push(CallMeta {
            kind: "native",
            wallet: w.clone(),
            token: None,
        });

        call_tokens.push(
            Token::Tuple(
                vec![
                    Token::Address(parse_h160(MULTICALL3_ADDR)?), // target = Multicall3 itself
                    Token::Bool(true), // allowFailure
                    Token::Bytes(calldata)
                ]
            )
        );
    }

    // erc20 calls
    for w in wallets {
        let w_addr = parse_h160(w)?;
        for t in token_contracts {
            let t_addr = parse_h160(t)?;
            let calldata = bal_of.encode_input(&[Token::Address(w_addr)])?;

            call_metas.push(CallMeta {
                kind: "erc20",
                wallet: w.clone(),
                token: Some(t.clone()),
            });

            call_tokens.push(
                Token::Tuple(
                    vec![
                        Token::Address(t_addr), // target = token contract
                        Token::Bool(true), // allowFailure
                        Token::Bytes(calldata) // callData
                    ]
                )
            );
        }
    }

    // Batch into multiple multicalls if too large
    let mut native: HashMap<String, U256> = HashMap::new();
    let mut erc20: HashMap<String, HashMap<String, U256>> = HashMap::new();

    let mut i = 0usize;
    while i < call_tokens.len() {
        let end = (i + max_calls_per_batch).min(call_tokens.len());
        let chunk_tokens = call_tokens[i..end].to_vec();
        let chunk_metas = call_metas[i..end].to_vec();

        let input = agg.encode_input(&[Token::Array(chunk_tokens)])?;
        let data_hex = format!("0x{}", hex::encode(input));

        let raw = rpc.eth_call(MULTICALL3_ADDR, &data_hex).await?;
        let raw = raw.strip_prefix("0x").unwrap_or(&raw);
        let out_bytes = hex::decode(raw)?;

        let decoded = agg.decode_output(&out_bytes)?;
        // decoded[0] = Array(Tuple(success, bytes))
        let results = match decoded.get(0) {
            Some(Token::Array(arr)) => arr,
            _ => {
                return Err(anyhow!("unexpected aggregate3 decode shape"));
            }
        };

        for (idx, item) in results.iter().enumerate() {
            let meta = &chunk_metas[idx];

            let (success, returndata) = match item {
                Token::Tuple(items) if items.len() == 2 => {
                    let s = matches!(items[0], Token::Bool(true));
                    let bytes = match &items[1] {
                        Token::Bytes(b) => b.clone(),
                        _ => vec![],
                    };
                    (s, bytes)
                }
                _ => (false, vec![]),
            };

            let value = if success && !returndata.is_empty() {
                u256_from_return_data(&returndata)
            } else {
                U256::zero()
            };

            match meta.kind {
                "native" => {
                    native.insert(meta.wallet.clone(), value);
                }
                "erc20" => {
                    let token = meta.token.clone().unwrap();
                    erc20.entry(meta.wallet.clone()).or_default().insert(token, value);
                }
                _ => {}
            }
        }

        i = end;
    }

    Ok(EvmBalances { native, erc20 })
}
