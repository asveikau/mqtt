#!/usr/bin/env perl
#
# Copyright (C) 2023 Andrew Sveikauskas
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.

use strict;
use List::Util qw(any);

my $messageName;
my $id;
my @fields;
my @indentstack;
my @condstack;
my @liststack;
my %nameAndIdPairs;

my %types = (
   'string' => {'ctype'=>'MqttString*',  'reader'=>'MqttReadString'},
   'blob'   => {'ctype'=>'MqttBlob*',    'reader'=>'MqttReadBlob'},
   'byte'   => {'ctype'=>'unsigned char','reader'=>'MqttReadByte'},
   'int16'  => {'ctype'=>'uint16_t',     'reader'=>'MqttReadU16'},
   'int32'  => {'ctype'=>'uint32_t',     'reader'=>'MqttReadU32'},
   'vbi'    => {'ctype'=>'uint64_t',     'reader'=>'MqttReadVbi'}
   # TODO: stringpair?
);

my $typesRegex = join('|', keys %types);

open(STRUCTHEADER, ">include/mqtt/generated/messages.h") || die;
open(PARSERFILE,   ">src/lib/generated/messages-parser.c") || die;
open(WRITERFILE,   ">src/lib/generated/messages-writer.c") || die;

my $disclaimer = "// This file is generated from message-types " .
                    "by gen-message-types.pl\n\n";
print STRUCTHEADER $disclaimer;
print PARSERFILE $disclaimer;
print WRITERFILE $disclaimer;

print PARSERFILE "// Put a breakpoint here to debug parse errors:\n";
print PARSERFILE "static bool\nparseError(void)\n{\n   return false;\n}\n\n";

sub flushMsg
{
   my $field;
   my $count = 0;
   my $hasProps = 0;

   $nameAndIdPairs{$messageName} = $id;

   print STRUCTHEADER "typedef struct\n" .
                      "{\n";
   my $indent;
   foreach $field (@fields)
   {
      my $type = ${$field}{'type'};
      my $name = ${$field}{'name'};
      my $defName = $name;

      if (!$defName)
      {
         $defName = "Prop_";
      }

      $count++;

      if ($type eq 'properties')
      {
         print STRUCTHEADER "   int $name"."nProps;\n" .
                            "   MqttProperty *$name"."Props;\n";
         my $propsList = ${$field}{'props'};

         if (!$propsList)
         {
            my @arr = ();
            $propsList = \@arr;
         }
         foreach my $prop (@$propsList)
         {
            my $subType = ${$prop}{'type'};
            my $subId = ${$prop}{'prop'};
            my $ctype = $types{$subType}{'ctype'};
            my $star = '*';
            if ($subType =~ /^(string|blob)$/)
            {
               $star = '';
            }
            print STRUCTHEADER "   $types{$subType}{'ctype'} $star$defName" . "$subId;\n";
         }
         $hasProps = 1;
      }
      elsif ($type eq 'list')
      {
         if (${$field}{'count'} == 1)
         {
            $indent = '';

            my $nameRef = \${${$field}{'lastSeenEntry'}}{'name'};
            $$nameRef = $$nameRef . "List";
         }
         else
         {
            $indent = '   ';
            print STRUCTHEADER "   struct\n" .
                               "   {\n";
         }
      }
      elsif ($type eq 'endlist')
      {
         my $listEntry = ${$field}{'listEntry'};
         $indent = '';
         if (${$listEntry}{'count'} == 1)
         {
            print STRUCTHEADER "   int ListSize;\n";
         }
         else
         {
            print STRUCTHEADER "   } *List;\n" .
                               "   int ListSize;\n";
         }
      }
      else
      {
         my $listEntry = ${$field}{'listEntry'};
         my $namePrefix;
         if ($listEntry && ${$listEntry}{'count'} == 1)
         {
            $namePrefix = '*';
         }
         print STRUCTHEADER "$indent   " . $types{$type}{'ctype'} . ' ' . "$namePrefix$name;\n";
      }
   }
   if ($count == 0)
   {
      print STRUCTHEADER "   int empty_struct_workaround;\n";
   }
   print STRUCTHEADER "} Mqtt" . $messageName . "Packet;\n\n";

   my $parseDecl = "bool\n" .
                   'MqttParse' . $messageName . 'Packet(' . "\n" .
                   '   struct MqttReader_ *reader,' . "\n" .
                   '   MqttParserState *state,' . "\n" .
                   '   MqttFixedHeader *Header,' . "\n" .
                   '   Mqtt' . $messageName . 'Packet *Packet' . "\n" .
                   ')';
   print STRUCTHEADER "$parseDecl;\n\n";

   my $writeDecl = "bool\n" .
                   'MqttWrite' . $messageName . 'Packet(' . "\n" .
                   '   struct MqttWriter_ *writer,' . "\n" .
                   '   MqttFixedHeader *Header,' . "\n" .
                   '   Mqtt' . $messageName . 'Packet *Packet' . "\n" .
                   ')';
   print STRUCTHEADER "$writeDecl;\n\n";

   # Can use typedef in later use.
   $parseDecl =~ s/struct MqttReader_/MqttReader/g;
   $writeDecl =~ s/struct MqttWriter_/MqttWriter/g;

   my $listPrefix;
   my $listSuffix;
   my $listPrefixWrite;
   my $listSuffixWrite;
   my $indent;

   print PARSERFILE "$parseDecl\n{\n";
   print WRITERFILE "$writeDecl\n{\n";

   if ($hasProps)
   {
      print PARSERFILE "   MqttReader propReader, readerBak;\n" .
                       "   int n;\n" .
                       "   uint64_t propId;\n" .
                       "   MqttPropertyType propType;\n" .
                       "   size_t propPayloadSize;\n";
                       "\n";

      print WRITERFILE "   int i;\n" .
                       "   MqttPropertyWriter propWriter;\n";
   }

   foreach $field (@fields)
   {
      my $type = ${$field}{'type'};
      my $name = ${$field}{'name'};
      my $cond = ${$field}{'cond'};
      my $reader = $types{$type}{'reader'};

      if ($type eq 'properties')
      {
         my $propsList = ${$field}{'props'};
         my @optionals = ('Disconnect');
         my $optional = 0; # was: any { $_ eq $messageName } @optionals;

         if (!($cond eq ''))
         {
            print PARSERFILE "   if (!($cond))\n" .
                             "      goto $name" . "skipProps;\n";
            print WRITERFILE "   if (!($cond))\n" .
                             "      goto $name" . "skipProps;\n";
         }
         print PARSERFILE "   if (!MqttReadPropertyList(reader, &propReader))\n" .
                          "      return " . ($optional ? "true" : "parseError()") . ";\n" .
                          "   readerBak = propReader;\n" .
                          "   n = 0;\n" .
                          "   while (MqttReadProperty(&propReader, &propId, &propType, &propPayloadSize))\n" .
                          "   {\n" .
                          "      if (INT_MAX/sizeof(*Packet->$name"."Props) < n+1)\n" .
                          "         return parseError();\n" .
                          "      ++n;\n" .
                          "      MqttReadSkip(&propReader, propPayloadSize);\n" .
                          "   }\n" .
                          "   Packet->$name"."nProps = n;\n" .
                          "   if (n)\n" .
                          "   {\n" .
                          "      if (!pool_alloc(&state->pool, (void**)&Packet->$name"."Props, Packet->$name"."nProps*sizeof(*Packet->$name"."Props)))\n" .
                          "      {\n" .
                          "         state->status = MqttParseStatus_Nomem;\n" .
                          "         return false;\n" .
                          "      }\n" .
                          "      n = 0;\n" .
                          "      propReader = readerBak;\n" .
                          "      while (MqttReadProperty(&propReader, &propId, &propType, &propPayloadSize))\n" .
                          "      {\n" .
                          "         readerBak = propReader;\n" .
                          "         readerBak.remaining = propPayloadSize;\n" .
                          "         if (!MqttParseProperty(&readerBak, propId, propType, Packet->$name"."Props + n))\n" .
                          "            return parseError();\n";

         print WRITERFILE "   if (!MqttBeginPropertyList(writer, &propWriter))\n" .
                          "      return false;\n" .
                          "   for (i=0; i<Packet->$name"."nProps; ++i)\n" .
                          "   {\n" .
                          "      if (!MqttWriteProperty(&propWriter, &Packet->$name"."Props[i]))\n" .
                          "         return false;\n" .
                          "   }\n";

         if ($propsList)
         {
            print PARSERFILE
                          "         void *ptr = NULL;\n" .
                          "         switch ((MqttPropertyId)propId)\n" .
                          "         {\n";
            foreach my $prop (@$propsList)
            {
               my $propId = ${$prop}{'prop'};
               my $propType = ${$prop}{'type'};
               my $defName = $name;

               $propType =~ s/^stringpair$/stringPair/;
               $propType =~ s/^int/i/;
               $propType =~ s/^\b(\w)/\U$1/;

               my $indir;
               my $indir_amp = '&';
               my $indir_star = '*';
               if ($propType =~ /^(String|Blob)$/)
               {
                  $indir = 1;
                  $indir_amp = '';
                  $indir_star = '';
               }

               my $propTypeAlt = $propType;
               $propTypeAlt =~ s/^I([0-9])/U$1/;

               if (!$defName)
               {
                  $defName = "Prop_";
               }

               print PARSERFILE
                          "         case $propId:\n" .
                          "            Packet->$defName$propId = $indir_amp" . "Packet->$name"."Props[n].$propType;\n";
               if (!$indir)
               {
                  print PARSERFILE
                          "            ptr = &Packet->$defName$propId;\n";
               }
               print PARSERFILE
                          "            break;\n";

               print WRITERFILE
                          "   if (Packet->$defName$propId)\n" .
                          "   {\n" .
                          "      MqttProperty tmpProp;\n" .
                          "      tmpProp.Id = $propId;\n" .
                          "      tmpProp.Type = $propTypeAlt;\n" .
                          "      tmpProp.$propType = $indir_star" . "Packet->$defName$propId;\n" .
                          "      if (!MqttWriteProperty(&propWriter, &tmpProp))\n" .
                          "         return false;\n" .
                          "   }\n";
            }
            print PARSERFILE
                          "         default:\n" .
                          "            break;\n" .
                          "         }\n" .
                          "         if (ptr && !pool_add_ptr(&state->pool, ptr))\n" .
                          "         {\n" .
                          "            state->status = MqttParseStatus_Nomem;\n" .
                          "            return false;\n" .
                          "         }\n";
         }


         print PARSERFILE "         ++n;\n" .
                          "         MqttReadSkip(&propReader, propPayloadSize);\n" .
                          "      }\n" .
                          "   }\n";
         print WRITERFILE "   if (!MqttEndPropertyList(&propWriter))\n" .
                          "      return false;\n";
         if (!($cond eq ''))
         {
            print PARSERFILE "$name" . "skipProps:\n";
            print WRITERFILE "$name" . "skipProps:\n";
         }
         next;
      }
      elsif ($type eq 'list')
      {
         my $count = ${$field}{'count'};
         my $listName = 'List';

         if ($count == 1)
         {
            $listName = ${${$field}{'lastSeenEntry'}}{'name'};
            $listSuffix = '[mult*n]';
            $listSuffixWrite = '[i]';
         }
         else
         {
            $listPrefix = 'List[mult*n].';
            $listPrefixWrite = 'List[i].';
         }
         ${$field}{'listName'} = $listName;

         $indent = '      ';
         print PARSERFILE "   for(int mult=0; mult<2; ++mult)\n" .
                          "   {\n" .
                          "      int64_t tmp[(sizeof(*Packet->$listName) + 15)/16 * 16 / 8];\n" .
                          "      readerBak = *reader;\n" .
                          "      if (mult)\n" .
                          "      {\n" .
                          "         if (!pool_alloc(&state->pool, (void**)&Packet->$listName, n*sizeof(*Packet->$listName)))\n" .
                          "         {\n" .
                          "            state->status = MqttParseStatus_Nomem;\n" .
                          "            return false;\n" .
                          "         }\n" .
                          "         Packet->ListSize = n;\n" .
                          "      }\n" .
                          "      else\n" .
                          "      {\n" .
                          "         Packet->$listName = (void*)tmp;\n" .
                          "      }\n" .
                          "      n = 0;\n" .
                          "      while (reader->remaining)\n" .
                          "      {\n";
         print WRITERFILE "   for(i=0; i<Packet->ListSize; ++i)\n" .
                          "   {\n";
      }
      elsif ($type eq 'endlist')
      {
         my $listEntry = ${$field}{'listEntry'};
         my $listName = ${$listEntry}{'listName'};
         $listPrefix = '';
         $listSuffix = '';
         $listPrefixWrite = '';
         $listSuffixWrite = '';
         $indent = '';
         print PARSERFILE "         if (INT_MAX/sizeof(*Packet->$listName) < n+1)\n" .
                          "            return parseError();\n" .
                          "         ++n;\n" .
                          "      }\n" .
                          "      *reader = readerBak;\n" .
                          "   }\n";
         print WRITERFILE "   }\n";
      }
      else
      {
         print PARSERFILE "$indent   if (";
         if (!($cond eq ''))
         {
            print PARSERFILE "$cond && ";
         }
         print PARSERFILE "!$reader(reader, &Packet->$listPrefix$name$listSuffix))\n" .
                          "$indent      return parseError();\n";

         my $writer = $reader;
         $writer =~ s/Read/Write/;
         my $writeIndent = $indent;
         $writeIndent =~ s/^   //;

         print WRITERFILE "$writeIndent   if (";
         if (!($cond eq ''))
         {
            print WRITERFILE "$cond && ";
         }
         print WRITERFILE "!$writer(writer, Packet->$listPrefixWrite$name$listSuffixWrite))\n" .
                          "$writeIndent      return false;\n";
      }
   }
   print PARSERFILE "   return true;\n" .
                    "}\n\n";
   print WRITERFILE "   return true;\n" .
                    "}\n\n";

   # Reset for next iteration
   $id = '';
   @fields = ();
   @condstack = ();
}

sub pushField
{
   my $entry = $_[0];
   if (scalar(@condstack) != 0)
   {
      ${$entry}{'cond'} = $condstack[scalar(@condstack) - 1]{'cond'};
   }
   if (scalar(@liststack) != 0)
   {
      my $listEntry = $liststack[scalar(@liststack) - 1];
      ${$entry}{'listEntry'} = $listEntry;

      if (!(${$entry}{'type'} eq 'endlist'))
      {
         ${$listEntry}{'lastSeenEntry'} = $entry;
      }
   }
   push(@fields, $entry);
}

while (<>)
{
   chomp;
   s/#.*$//; # remove comments

   if (/\s*\[\s*(\w+[\w\s]*)\s*\]\s*/)
   {
      if (scalar(@liststack) != 0)
      {
         my %entry = ( 'type' => 'endlist' );
         pushField(\%entry);
         pop(@liststack);
      }

      # New message type
      $_ = $1;
      s/\s*$//;
      my $newMessage = $_;
      if (!($messageName eq ''))
      {
         flushMsg();
      }
      $messageName = $newMessage;
      next;
   }

   # Normalize some whitespace
   s/^([\s]*)//;
   my $indent = $1;   # TODO: use this
   s/\s*$//;
   s/\s+/ /g;

   if ($_ eq '')
   {
      next;
   }

   if (scalar(@condstack) != 0)
   {
      my $stackIndent = $condstack[scalar(@condstack)-1]{'indent'};
      if ($indent eq $stackIndent)
      {
         pop(@condstack);
      }
   }

   if (scalar(@liststack) != 0)
   {
      my $stackIndent = $liststack[scalar(@liststack)-1]{'indent'};
      if ($indent eq $stackIndent)
      {
         my %entry = ( 'type' => 'endlist' );
         pushField(\%entry);
         pop(@liststack);
      }
   }

   if (/^id (.*)$/)
   {
      $id = $1;
   }
   elsif (/^($typesRegex) (.+)$/)
   {
      my %entry =
      (
         'type' => $1,
         'name' => $2
      );
      if (scalar(@liststack) != 0)
      {
         my $entryRef = $liststack[scalar(@liststack) - 1];
         ${$entryRef}{'count'}++;
      }
      pushField(\%entry);
   }
   elsif (/^(properties)(| .*)$/)
   {
      my %entry = ('type' => $1);
      my $name = $2;
      $name =~ s/^ *//g;
      if (!($name eq ''))
      {
         $entry{'name'} = $name . "_";
      }
      pushField(\%entry);
   }
   elsif (/^prop ([^ ]*) ([^ ]*)$/)
   {
      my $type = $1;
      my $prop = $2;
      my $propEntry = $fields[scalar(@fields) - 1];
      my $propList = ${$propEntry}{'props'};
      if (!$propList)
      {
         my @arr = ();
         $propList = \@arr;
         ${$propEntry}{'props'} = $propList;
      }
      my %entry = ('type' => $type, 'prop' => $prop);
      push (@$propList, \%entry);
   }
   elsif (/^cond (.+)$/)
   {
      my %entry = ('cond' => $1, 'indent' => $indent );
      push(@condstack, \%entry);
   }
   elsif ($_ eq 'list')
   {
      my %entry = ('indent' => $indent);
      push(@liststack, \%entry);
      %entry = ('type' => 'list');
      pushField(\%entry);
   }
}

if (!($messageName eq ''))
{
   flushMsg();
}

print STRUCTHEADER "typedef union\n" .
                   "{\n";
foreach my $name (keys %nameAndIdPairs)
{
   print STRUCTHEADER "   Mqtt" . $name . "Packet $name;\n";
}
print STRUCTHEADER "} MqttPacketStorage;\n\n";

print PARSERFILE "static bool\n" .
                 "MqttParsePacketDispatch(\n" .
                 "   MqttReader *reader,\n" .
                 "   MqttParserState *state,\n" .
                 "   MqttFixedHeader *Header,\n" .
                 "   MqttPacketStorage *out\n" .
                 ")\n" .
                 "{\n" .
                 "   switch (Header->Type)\n".
                 "   {\n";

print WRITERFILE "static bool\n" .
                 "MqttWritePacketDispatch(\n" .
                 "   MqttWriter *writer,\n" .
                 "   MqttFixedHeader *Header,\n" .
                 "   MqttPacketStorage *in\n" .
                 ")\n" .
                 "{\n" .
                 "   switch (Header->Type)\n".
                 "   {\n";

while (my ($name, $id) = each(%nameAndIdPairs))
{
   print PARSERFILE "   case $id:\n" .
                    "      return MqttParse$name" . "Packet(reader, state, Header, &out->$name);\n";

   print WRITERFILE "   case $id:\n" .
                    "      return MqttWrite$name" . "Packet(writer, Header, &in->$name);\n";
}

print PARSERFILE "   }\n" .
                 "   return parseError();\n" .
                 "}\n";

print WRITERFILE "   }\n" .
                 "   return false;\n" .
                 "}\n";

close STRUCTHEADER;
close PARSERFILE;
