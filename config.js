require('dotenv').config()

const {Pool} = require('pg')
const isProduction = process.env.NODE_ENV === 'production'

const connectionString = `postgres://kgqjbddwuyzhgm:2bcecef14aa7dcf7adbb3a62df15f69cd07caacc4afe8faf476cf059ac02b13d@ec2-54-155-22-153.eu-west-1.compute.amazonaws.com:5432/dbc25ri2361c0p`

const pool = new Pool({
    connectionString: isProduction ? process.env.DATABASE_URL : connectionString,
    ssl: {
        rejectUnauthorized: false
    }
})

const API_KEY = isProduction? process.env.API_KEY : 'novocaineikeepitcoming';

module.exports = {pool, API_KEY}