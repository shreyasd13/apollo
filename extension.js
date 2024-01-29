import {
	ApolloServer
} from '@apollo/server';

import { parse } from 'node:url';
import {isMainThread} from 'node:worker_threads';
import { pathToFileURL } from 'url';

const {GraphQL} = databases.cache;

let graphql_schema = `
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
export function start(options) {
	apollo_options = options;
}
export async function handleFile(content, url_path, file_path, resources) {
	if (file_path.endsWith('.graphql'))
		graphql_schema += content;
	if (file_path.endsWith('.js')) {
		const module_url = pathToFileURL(file_path).toString();
		// load JS file and assign to resolvers
		const module_exports = await import(module_url);
		Object.assign(resolvers, module_exports.default || module_exports);
	}
}
export async function ready() {
	if (isMainThread || apollo) return;
	apollo = new ApolloServer({
		typeDefs: graphql_schema,
		resolvers,
		cache: new HarperDBCache()
	})
	await apollo.start();
	server.http(async (request, next_handler) => {
		if (request.url === '/graphql') {
			let body = await streamToBuffer(request.body);
			request = Object.assign({}, request);
			request.body = JSON.parse(body);
			request.search = parse(request.url).search || '';
			let response = await apollo.executeHTTPGraphQLRequest({
				httpGraphQLRequest: request,
				context: () => request,
			});
			response.body = response.body.string;
			return response;
		} else return next_handler(request);
	}, apollo_options);
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
