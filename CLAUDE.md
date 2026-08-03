# CLAUDE.md — Nautilus Trader Algorithmic Trading Platform

Work alongside the project description when making answers. Whenever answering on this
project refer to me as Damen. Never include knowledge about me from external chats not in
this chat.

If you are unsure about an answer to a problem or prompt say you do not have a sound
answer or solution to the problem. Do not hallucinate or make up answers for the sake of
answering.

Back your answers by scientific research wherever possible. Cite the research paper or
website where you based your findings from.

This project is algorithmic trading based. A lot of content on this topic online is from
fake trading gurus or trading hype pages. Stay away from wish-wash content like this
whenever researching your answers. This includes anything to do with smart money
concepts, ICT concepts or anything out of the "day trading community". Some day trading
concepts are useful but be mindful of the source they come from. For example, there is a
fine line between day trading technical chart analysis and technical indicators based on
mathematical principles that can be used in algorithmic trading.

The Nautilus Data Pipeline is event-driven. There is no external database but rather a
Parquet data catalog optimised for Nautilus ingestion. When answering technical questions
regarding coding implementation the key paradigm to frame your thoughts is through the
event driven paradigm, meaning feature computation must be written in an event driven
manner, and machine learning based features or predictions must make use of incremental
batch learning via a side job and then written back to the event driven pipeline.

The main nautilus event loop is mono-threaded. Keep this in mind when thinking about
architectural decisions and how to efficiently implement machine learning models or other
computationally expensive functions.

While simplicity is key, this is never to be at the sacrifice of data integrity,
execution integrity, strategy logic and integrity, profitability, data pipeline
integrity, and general sound algorithmic trading principles.

For the most part, volume bars will be the main premise for strategy research and feature
computation. Keep this in mind when discussing concepts and strategies. Specify where it
may be necessary to deviate or whether additional aspects must be considered regarding
statistical properties or scale alignment.

Wherever possible, data transformation should be implemented such that the data used in
the strategy and for machine learning training is as close to identically and
independently Gaussian distributed and with stationary distributional
characteristics/parameters.

Keep code as simple as possible and include concise comments and doc strings. Never
convolute these.

I am using a 24gb unified memory 10 core CPU 10 core GPU with a neural engine M4 macbook
Air with 500gb RAM but have a 1TB HDD available. Ideally, once core development is
complete, I am looking to move to AWS cloud for Bybit exchange execution.

I have 300GB of market data for ETHUSDT, BTCUSDT, SOLUSDT, LINKUSDT on the HDD but i have
ETHUSDT immediately available on disk with funding rate, order book L2 mbo, kline,
premium kline and tick data.

## Change discipline

Make minimal, localized changes only. Prefer the smallest diff that solves the problem.
Never perform broad or multi-file refactors, rename widely, or touch code unrelated to
the specific task without asking me first and getting explicit approval. If a task seems
to require a large or structural change, stop and explain the proposed change and why,
then wait for my go-ahead before editing. Do not reformat, reorganize, or "clean up"
code I did not ask you to change.
