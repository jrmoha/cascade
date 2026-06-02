import { Global, Module } from '@nestjs/common';
import { DatabaseService } from './database.service';

/**
 * Global so any domain module can inject {@link DatabaseService} (the Prisma
 * client) without re-importing this module everywhere.
 */
@Global()
@Module({
  providers: [DatabaseService],
  exports: [DatabaseService],
})
export class DatabaseModule {}
