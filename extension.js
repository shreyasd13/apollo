import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

import { ApolloServer, HeaderMap } from '@apollo/server';
import fastGlob from "fast-glob";

const { GraphQL } = databases.cache;

const BASE_SCHEMA = `#graphql
enum CacheControlScope {
  PUBLIC
  PRIVATE
}

directive @cacheControl(
  maxAge: Int
  scope: CacheControlScope
  inheritMaxAge: Boolean
) on FIELD_DEFINITION | OBJECT | INTERFACE | UNION

directive @table(
	database: String 
	table: String
	expiration: Int
	audit: Boolean
) on OBJECT

directive @export(
	name: String
) on OBJECT

directive @sealed on OBJECT
directive @primaryKey on FIELD_DEFINITION
directive @indexed on FIELD_DEFINITION
directive @updatedTime on FIELD_DEFINITION
directive @relationship(
	to: String
	from: String
) on FIELD_DEFINITION

scalar Long
scalar BigInt
scalar Date
scalar Any
`;

let resolvers = {};
let apollo_options;
let apollo;

export function start(options = {}) {
	const config = {
		cache: options.cache,
		port: options.port,
		resolvers: options.resolvers ?? './resolvers.js',
		schemas: options.schemas ?? './schemas.graphql',
		securePort: options.securePort,
	}

	logger.debug('@harperdb/apollo extension configuration:\n' + JSON.stringify(config, null, 2));

	return {
		async handleDirectory(_, componentPath) {

			// Load the resolvers
			const resolversPath = join(componentPath, config.resolvers);
			console.log("Resolver path : ");
			console.log(config.resolvers);
			
			const resolvers = await import(pathToFileURL(resolversPath));
			console.log("RESOLVERS : ");
			console.log(resolvers);

			// Load the schemas
			const schemasPath = join(componentPath, config.schemas)
			console.log("schemasPath : ", schemasPath);
			let typeDefs = BASE_SCHEMA;
			for (const filePath of fastGlob.sync(fastGlob.convertPathToPattern(schemasPath), { onlyFiles: true })) {
				console.log("filePath : ", filePath);
				typeDefs += readFileSync(filePath, 'utf-8');
			}

			// Get the custom cache or use the default
			const Cache = config.cache ? await import(pathToFileURL(join(componentPath, config.cache))) : HarperDBCache;

			// Set up Apollo Server
			const apollo = new ApolloServer({ typeDefs, resolvers: resolvers.default || resolvers, cache: new Cache() });

			await apollo.start();

			server.http(
				async (request, next) => {
					const url = new URL(request.url, `http://${process.env.HOST ?? 'localhost'}`);
					if (url.pathname === '/graphql') {
						const body = await streamToBuffer(request.body);

						const httpGraphQLRequest = {
							method: request.method,
							headers: new HeaderMap(request.headers),
							body: JSON.parse(body),
							search: url.search,
						};

						const response = await apollo.executeHTTPGraphQLRequest({
							httpGraphQLRequest: httpGraphQLRequest,
							context: () => httpGraphQLRequest
						});
						response.body = response.body.string;
						return response;
					} else {
						return next(request);
					}
				},
				{ port: config.port, securePort: config.securePort }
			);

			return true;
		}
	}
}

function streamToBuffer(stream) {
	return new Promise((resolve, reject) => {
		const buffers = [];
		stream.on('data', (data) => buffers.push(data));
		stream.on('end', () => resolve(Buffer.concat(buffers)));
		stream.on('error', reject);
	});
}

class HarperDBCache extends Resource {

	async get(key){
			let data = await GraphQL.get(key);
			return data?.get('query');
	}

	async set(key, value, options){
		let context = this.getContext();
		if(options?.ttl) {
			if(!context) {
				context = {};
			}
			//the ttl is in seconds
			context.expiresAt = Date.now() + (options.ttl * 1000);
		}

		await GraphQL.put({ id: key, query: value }, context);
	}

	async delete(key){
		await GraphQL.delete(key);
	}
}
