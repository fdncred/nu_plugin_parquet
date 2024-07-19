# From jakeswenson

This repo is a copy from jakeswenson's repo and updated to support nushell v0.60+ by @flying-sheep. See the original here https://github.com/jakeswenson/nu_plugin_from_parquet. Asked permission to fork, update, and add license here https://github.com/jakeswenson/nu_plugin_from_parquet/issues/4

# nu_plugin_parquet

[nushell]: https://www.nushell.sh/
[plugin]: https://www.nushell.sh/book/plugins.html#adding-a-plugin
[structured types]: https://www.nushell.sh/book/types_of_data.html

This is a [nushell] [plugin] to add parquet compatibility with `nu` structured types. It can read parquet files to `nu` tables, or write tables to parquet files.


# Installing

[add the plugin]: https://www.nushell.sh/book/plugins.html#adding-a-plugin

To [add the plugin] permanently, just install it and call `plugin add` on it:

## Using Cargo

```nushell
cargo install --path .
plugin add ~/.cargo/bin/nu_plugin_parquet
plugin use ~/.cargo/bin/nu_plugin_parquet # required if you don't want to quit out and restart nushell
```

## Usage
### Reading

```nushell
open -r sample.parquet | from parquet | first 10
```
or
```nushell
open sample.parquet | first 10
```

```nushell
╭───┬───────────────┬────┬────────────┬───────────┬──────────────┬────────┬──────────────┬──────────────┬──────────────┬────────────┬───────────┬──────────────┬──────────╮
│ # │ registration… │ id │ first_name │ last_name │    email     │ gender │  ip_address  │      cc      │   country    │ birthdate  │  salary   │    title     │ comments │
├───┼───────────────┼────┼────────────┼───────────┼──────────────┼────────┼──────────────┼──────────────┼──────────────┼────────────┼───────────┼──────────────┼──────────┤
│ 0 │ 6 years ago   │  1 │ Amanda     │ Jordan    │ ajordan0@co… │ Female │ 1.197.201.2  │ 67595218649… │ Indonesia    │ 3/8/1971   │  49756.53 │ Internal Au… │ 1E+02    │
│ 1 │ 6 years ago   │  2 │ Albert     │ Freeman   │ afreeman1@i… │ Male   │ 218.111.175… │              │ Canada       │ 1/16/1968  │ 150280.17 │ Accountant … │          │
│ 2 │ 6 years ago   │  3 │ Evelyn     │ Morgan    │ emorgan2@al… │ Female │ 7.161.136.94 │ 67671190719… │ Russia       │ 2/1/1960   │ 144972.51 │ Structural … │          │
│ 3 │ 6 years ago   │  4 │ Denise     │ Riley     │ driley3@gmp… │ Female │ 140.35.109.… │ 35760315989… │ China        │ 4/8/1997   │  90263.05 │ Senior Cost… │          │
│ 4 │ 6 years ago   │  5 │ Carlos     │ Burns     │ cburns4@mii… │        │ 169.113.235… │ 56022562552… │ South Africa │            │           │              │          │
│ 5 │ 6 years ago   │  6 │ Kathryn    │ White     │ kwhite5@goo… │ Female │ 195.131.81.… │ 35831363260… │ Indonesia    │ 2/25/1983  │  69227.11 │ Account Exe… │          │
│ 6 │ 6 years ago   │  7 │ Samuel     │ Holmes    │ sholmes6@fo… │ Male   │ 232.234.81.… │ 35826413669… │ Portugal     │ 12/18/1987 │  14247.62 │ Senior Fina… │          │
│ 7 │ 6 years ago   │  8 │ Harry      │ Howell    │ hhowell7@ee… │ Male   │ 91.235.51.73 │              │ Bosnia and … │ 3/1/1962   │ 186469.43 │ Web Develop… │          │
│ 8 │ 6 years ago   │  9 │ Jose       │ Foster    │ jfoster8@ye… │ Male   │ 132.31.53.61 │              │ South Korea  │ 3/27/1992  │ 231067.84 │ Software Te… │ 1E+02    │
│ 9 │ 6 years ago   │ 10 │ Emily      │ Stewart   │ estewart9@o… │ Female │ 143.28.251.… │ 35742541103… │ Nigeria      │ 1/28/1997  │  27234.28 │ Health Coac… │          │
├───┼───────────────┼────┼────────────┼───────────┼──────────────┼────────┼──────────────┼──────────────┼──────────────┼────────────┼───────────┼──────────────┼──────────┤
│ # │ registration… │ id │ first_name │ last_name │    email     │ gender │  ip_address  │      cc      │   country    │ birthdate  │  salary   │    title     │ comments │
╰───┴───────────────┴────┴────────────┴───────────┴──────────────┴────────┴──────────────┴──────────────┴──────────────┴────────────┴───────────┴──────────────┴──────────╯
```

### Displaying Metadata

Display metadata, instead of data, from the parquet file by passing the `--metadata, -m` flag to `from parquet`:

```nushell
open -r sample.parquet | from parquet --metadata  | table -e
```

```nushell
╭────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ version    │ 1                                                                                                   │
│ creator    │ parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf)                           │
│ num_rows   │ 1000                                                                                                │
│ key_values │ [list 0 items]                                                                                      │
│            │ ╭─────────────┬───────────────────────────────────────────────────────────────────────────────────╮ │
│ schema     │ │ name        │ hive_schema                                                                       │ │
│            │ │ num_columns │ 13                                                                                │ │
│            │ │             │ ╭────┬───────────────────┬────────────┬────────────┬─────────────┬──────────────╮ │ │
│            │ │ schema      │ │  # │       name        │ repetition │    type    │ type_length │ logical_type │ │ │
│            │ │             │ ├────┼───────────────────┼────────────┼────────────┼─────────────┼──────────────┤ │ │
│            │ │             │ │  0 │ registration_dttm │ OPTIONAL   │ INT96      │             │              │ │ │
│            │ │             │ │  1 │ id                │ OPTIONAL   │ INT32      │             │              │ │ │
│            │ │             │ │  2 │ first_name        │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │  3 │ last_name         │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │  4 │ email             │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │  5 │ gender            │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │  6 │ ip_address        │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │  7 │ cc                │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │  8 │ country           │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │  9 │ birthdate         │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │ 10 │ salary            │ OPTIONAL   │ DOUBLE     │             │              │ │ │
│            │ │             │ │ 11 │ title             │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ │ 12 │ comments          │ OPTIONAL   │ BYTE_ARRAY │          -1 │ UTF8         │ │ │
│            │ │             │ ╰────┴───────────────────┴────────────┴────────────┴─────────────┴──────────────╯ │ │
│            │ ╰─────────────┴───────────────────────────────────────────────────────────────────────────────────╯ │
│            │ ╭───┬──────────┬─────────────────╮                                                                  │
│ row_groups │ │ # │ num_rows │ total_byte_size │                                                                  │
│            │ ├───┼──────────┼─────────────────┤                                                                  │
│            │ │ 0 │     1000 │          112492 │                                                                  │
│            │ ╰───┴──────────┴─────────────────╯                                                                  │
╰────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Writing

```bash
[{a:1, b:3}, {a: 2, b:4}] | save example.parquet
```

Or, to save all running processes: 
```bash
ps | save example.parquet
```
