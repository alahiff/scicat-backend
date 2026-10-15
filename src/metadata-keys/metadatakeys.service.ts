import { Injectable, Logger, Scope } from "@nestjs/common";
import { InjectModel } from "@nestjs/mongoose";
import {
  MetadataKeyClass,
  MetadataKeyDocument,
} from "./schemas/metadatakey.schema";
import { FilterQuery, Model, PipelineStage, QueryOptions } from "mongoose";
import { isEmpty } from "lodash";
import {
  addCreatedByFields,
  decodeURIComponentExtended,
  parsePipelineProjection,
  parsePipelineSort,
} from "src/common/utils";

type ScientificMetadataEntry = {
  human_name?: string;
};

export type MetadataSourceDoc = {
  sourceType: string;
  userGroups: string[];
  isPublished: boolean;
  metadata: Record<string, unknown>;
};

// Recomputes the userGroups string array from userGroupCounts after a decrement.
// Retains only group names whose reference count is still above zero.
const RECOMPUTE_USER_GROUPS_STAGE = [
  {
    $set: {
      userGroupCounts: {
        $arrayToObject: {
          $filter: {
            input: { $objectToArray: "$userGroupCounts" },
            cond: { $gt: ["$$this.v", 0] },
          },
        },
      },
    },
  },
  {
    $set: {
      userGroups: {
        $map: {
          input: { $objectToArray: "$userGroupCounts" },
          as: "entry",
          in: "$$entry.k",
        },
      },
    },
  },
];

@Injectable({ scope: Scope.REQUEST })
export class MetadataKeysService {
  constructor(
    @InjectModel(MetadataKeyClass.name)
    private metadataKeyModel: Model<MetadataKeyDocument>,
  ) {}

  async findAll(
    filter: FilterQuery<MetadataKeyDocument>,
    accessFilter: FilterQuery<MetadataKeyDocument>,
  ): Promise<MetadataKeyClass[]> {
    const whereFilter: FilterQuery<MetadataKeyDocument> = filter.where ?? {};
    const fieldsProjection: string[] = filter.fields ?? {};

    const limits: QueryOptions<MetadataKeyDocument> = {
      limit: filter.limits?.limit ?? 100,
      skip: filter.limits?.skip ?? 0,
      sort: filter.limits?.sort ?? { createdAt: "desc" },
    };

    const pipeline: PipelineStage[] = [
      {
        $match: {
          $and: [accessFilter, whereFilter],
        },
      },
    ];

    if (!isEmpty(fieldsProjection)) {
      const projection = parsePipelineProjection(fieldsProjection);
      pipeline.push({ $project: projection });
    }

    if (!isEmpty(limits.sort)) {
      const sort = parsePipelineSort(limits.sort);
      pipeline.push({ $sort: sort });
    }

    pipeline.push({ $skip: limits.skip || 0 });

    pipeline.push({ $limit: limits.limit || 100 });

    const data = await this.metadataKeyModel
      .aggregate<MetadataKeyClass>(pipeline)
      .exec();

    return data;
  }

  async insertManyFromSource(doc: MetadataSourceDoc): Promise<void> {
    await this.adjustCounts(doc, 1);
  }

  async deleteMany(doc: MetadataSourceDoc): Promise<void> {
    await this.adjustCounts(doc, -1);
  }

  async replaceManyFromSource(
    oldDoc: MetadataSourceDoc,
    newDoc: MetadataSourceDoc,
  ): Promise<void> {
    await this.deleteMany(oldDoc);
    await this.insertManyFromSource(newDoc);
  }

  private async adjustCounts(
    doc: MetadataSourceDoc,
    delta: 1 | -1,
  ): Promise<void> {
    if (isEmpty(doc.metadata)) return;

    const { sourceType, userGroups, isPublished, metadata } = doc;

    const filters = this.flattenMetadata(metadata).map(
      ({ key, humanReadableName }) => ({
        sourceType,
        key,
        humanReadableName,
      }),
    );

    if (filters.length === 0) return;

    const queryFilter = { $or: filters };

    const ops = filters.map(({ sourceType, key, humanReadableName }) => ({
      updateOne: {
        filter: { sourceType, key, humanReadableName },
        update: {
          $set: {
            updatedAt: new Date(),
          },
          $inc: {
            usageCount: delta,
            ...this.groupCountDeltas(userGroups, delta),
          },
          ...(delta === 1 && {
            $max: { isPublished },
            $addToSet: { userGroups: { $each: userGroups } },
            $setOnInsert: addCreatedByFields({}, "system"),
          }),
        },
        upsert: delta === 1,
      },
    }));

    await this.metadataKeyModel.bulkWrite(ops);

    if (delta === -1) {
      // UpdateMany is necessary here because the bulkWrite above only decrements userGroupCounts
      // but cannot remove zero-count groups from userGroups in the same operation.
      await this.metadataKeyModel.updateMany(
        queryFilter,
        RECOMPUTE_USER_GROUPS_STAGE,
      );

      await this.metadataKeyModel.deleteMany({
        $and: [queryFilter, { usageCount: { $lte: 0 } }],
      });
    }

    Logger.log(
      `${delta === 1 ? "Upserted" : "Decremented or deleted"} MetadataKeys for ${sourceType}: ${Object.keys(metadata).join(", ")}`,
    );
  }

  // Returns one entry per leaf, keyed by the dotted path (e.g. "a.b.c"). An object holding
  // "value" (optionally with unit/human_name) is a leaf; any other plain object is nesting.
  // Stored field names are URL-encoded for Mongo, so each segment is decoded here
  // (a literal "." inside a segment comes back as ".", same as the dataset keys endpoint).
  private flattenMetadata(
    metadata: Record<string, unknown>,
    prefix = "",
  ): { key: string; humanReadableName: string }[] {
    return Object.entries(metadata).flatMap(([rawKey, entry]) => {
      const key = `${prefix}${decodeURIComponentExtended(rawKey)}`;
      const isNested =
        entry !== null &&
        typeof entry === "object" &&
        !Array.isArray(entry) &&
        !("value" in entry);
      if (isNested) {
        return this.flattenMetadata(
          entry as Record<string, unknown>,
          `${key}.`,
        );
      }
      return [
        {
          key,
          humanReadableName:
            (entry as ScientificMetadataEntry | null)?.human_name ?? "",
        },
      ];
    });
  }

  private groupCountDeltas(
    groups: string[],
    delta: 1 | -1,
  ): Record<string, number> {
    return Object.fromEntries(
      groups.map((g) => [`userGroupCounts.${g}`, delta]),
    );
  }
}
