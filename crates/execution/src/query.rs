//! Read helpers over the Aptos state layout, served by the node's `GET /v1/accounts/:address`.

use anyhow::Result;
use aptos_types::{
    account_address::AccountAddress,
    account_config::{AccountResource, FungibleStoreResource, ObjectGroupResource, primary_apt_store},
    state_store::{StateView, state_key::StateKey},
};
use move_core_types::{language_storage::StructTag, move_resource::MoveStructType};
use std::collections::BTreeMap;

/// APT balance held in the account's primary fungible store (0 if it has none).
pub fn apt_balance(state: &impl StateView, address: AccountAddress) -> Result<u64> {
    let key = StateKey::resource_group(
        &primary_apt_store(address),
        &ObjectGroupResource::struct_tag(),
    );
    let Some(bytes) = state.get_state_value_bytes(&key)? else {
        return Ok(0);
    };
    let group: BTreeMap<StructTag, Vec<u8>> = bcs::from_bytes(&bytes)?;
    Ok(match group.get(&FungibleStoreResource::struct_tag()) {
        Some(store) => bcs::from_bytes::<FungibleStoreResource>(store)?.balance(),
        None => 0,
    })
}

/// Next sequence number the account must use (0 if the account resource does not exist yet).
pub fn sequence_number(state: &impl StateView, address: AccountAddress) -> Result<u64> {
    let key = StateKey::resource_typed::<AccountResource>(&address)?;
    Ok(match state.get_state_value_bytes(&key)? {
        Some(bytes) => bcs::from_bytes::<AccountResource>(&bytes)?.sequence_number(),
        None => 0,
    })
}
