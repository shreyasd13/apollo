import {
	ApolloServer
} from '@apollo/server';
import { parse } from 'node:url';
import {isMainThread} from 'node:worker_threads';
import { pathToFileURL } from 'url';

let graphql_schema = `
directive @table on OBJECT
directive @export on OBJECT
directive @primaryKey on FIELD_DEFINITION
directive @indexed on FIELD_DEFINITION
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
	});
}
function streamToBuffer(stream) {
	return new Promise((resolve, reject) => {
		const buffers = [];
		stream.on('data', (data) => buffers.push(data));
		stream.on('end', () => resolve(Buffer.concat(buffers)));
		stream.on('error', reject);
	});
}