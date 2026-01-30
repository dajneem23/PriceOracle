module.exports = {
    WORKER_VCB: {
        autoImport: true
    },
    WORKER_YAHOO: {
        interval: '1d',
        autoImport: true,
        wsEndpoint: process.env.WS_ENDPOINT || null
    },
    WORKER_REUTERS: {
        symbol: 'VND=X',
        autoImport: true,
        wsEndpoint: process.env.WS_ENDPOINT || null
    },
    WORKER_XE: {
        fromCurrency: "USD",
        toCurrency: "VND",
        autoImport: true,
        wsEndpoint: process.env.WS_ENDPOINT || null
    }
}