CREATE TABLE forex_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    pair VARCHAR(10) NOT NULL, -- Para almacenar el par de divisas (Ej: EUR-USD)
    timeframe VARCHAR(10) NOT NULL, -- Para almacenar la temporalidad (Ej: 4H, 15M, 3M)
    open NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    volume NUMERIC,
    tenkan_sen NUMERIC, -- Para Ichimoku en timeframe de 4H
    kijun_sen NUMERIC, -- Para Ichimoku en timeframe de 4H
    senkou_span_a NUMERIC, -- Para Ichimoku en timeframe de 4H
    senkou_span_b NUMERIC, -- Para Ichimoku en timeframe de 4H
    chikou_span NUMERIC, -- Para Ichimoku en timeframe de 4H
    upper_band NUMERIC, -- Para Bandas de Bollinger en 15M
    middle_band NUMERIC, -- Para Bandas de Bollinger en 15M
    lower_band NUMERIC, -- Para Bandas de Bollinger en 15M
    supertrend_low NUMERIC, -- Para Supertrend en 3M
    supertrend_high NUMERIC -- Para Supertrend en 3M
);
