import MongoConnection from "./db/connection.js";
import dotenv from "dotenv";

dotenv.config();
const {
  CONNECTION_STRING,
  DB_NAME,
  COLLECTION_NAME
} = process.env;

const connectionStr = `${CONNECTION_STRING}`;
const dbName = `${DB_NAME}`;
const mongoConnection = new MongoConnection(connectionStr, dbName);

async function connectToDatabase() {
  try {
    await mongoConnection.client.connect();
    console.log("Connected to the database successfully");
  } catch (err) {
    console.error("Failed to connect to the database:", err);
  }
}

connectToDatabase();

async function getFilteredMovies(pipeline) {
  try {
    return await mongoConnection.getCollection(COLLECTION_NAME).aggregate(pipeline).toArray();
  } catch (err) {
    console.error("Error fetching filtered movies:", err);
    throw err;
  }
}

async function runQueries() {
  try {
    // Pipeline for movies where imdb.rating is less than tomatoes.viewer.rating
    const pipeline1 = [
      { $match: { $expr: { $lt: ["$imdb.rating", "$tomatoes.viewer.rating"] } } }
    ];
    console.log("Pipeline 1 results:", await getFilteredMovies(pipeline1));

    // Pipeline for movies with exactly "Russian" as language
    const pipeline2 = [
      { $match: { languages: ["Russian"] } }
    ];
    console.log("Pipeline 2 results:", await getFilteredMovies(pipeline2));

    // Pipeline for movies having both "Action" and "Comedy" genres
    const pipeline3 = [
      { $match: { genres: { $all: ["Action", "Comedy"] } } }
    ];
    console.log("Pipeline 3 results:", await getFilteredMovies(pipeline3));

    // Pipeline for movies having neither "Action" nor "Comedy" genres
    const pipeline4 = [
      { $match: { genres: { $nin: ["Action", "Comedy"] } } }
    ];
    console.log("Pipeline 4 results:", await getFilteredMovies(pipeline4));

    // Pipeline for the top two movies with the maximum awards wins (projecting only title)
    const pipeline5 = [
      { $sort: { "awards.wins": -1 } },
      { $limit: 2 },
      { $project: { _id: 0, title: 1 } }
    ];
    console.log("Pipeline 5 results:", await getFilteredMovies(pipeline5));

    // Pipeline for grouping movies released in 2010 by imdb.rating with their titles
    const pipeline6 = [
      { $match: { year: 2010 } },
      { $group: { _id: "$imdb.rating", titles: { $push: "$title" } } }
    ];
    console.log("Pipeline 6 results:", await getFilteredMovies(pipeline6));
  } catch (error) {
    console.error("Error fetching filtered movies:", err);
    throw err;
  } finally {
    mongoConnection.close();
  }
}

runQueries();