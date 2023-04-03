# From jakeswenson

This repo is a copy from jakeswenson's repo and updated to support nushell v0.60+ by @flying-sheep. See the original here https://github.com/jakeswenson/nu_plugin_from_parquet. Asked permission to fork, update, and add license here https://github.com/jakeswenson/nu_plugin_from_parquet/issues/4

# nu_plugin_from_parquet

[nushell]: https://www.nushell.sh/
[plugin]: https://www.nushell.sh/contributor-book/plugins.html
[structured types]: https://www.nushell.sh/book/types_of_data.html

This is a [nushell] [plugin] to parse parquet data files into `nu` structured types.


# Installing

[add the plugin]: https://www.nushell.sh/book/plugins.html#adding-a-plugin
[`register`]: https://www.nushell.sh/book/commands/register.html

To [add the plugin] permanently, just install it and call [`register`] on it:

## Using Cargo

```bash
cargo install nu_plugin_from_parquet
register ~/.cargo/bin/nu_plugin_from_parquet
```

## Usage
```bash
open -r sample.parquet | from parquet | first 10
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

```bash
open -r sample.parquet | from parquet --metadata
╭────────────┬───────────────────────────────────────────────────────────────────────────╮
│ version    │ 1                                                                         │
│ creator    │ parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf) │
│ num_rows   │ 1000                                                                      │
│ key_values │ [list 0 items]                                                            │
│ schema     │ {record 3 fields}                                                         │
│ row_groups │ [table 1 row]                                                             │
╰────────────┴───────────────────────────────────────────────────────────────────────────╯
```

